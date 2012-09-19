<?php

namespace Resque\Pool;

use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Yaml\Exception\ParseException;

class Pool
{
    static private $SIG_QUEUE_MAX_SIZE = 5;
    static private $DEFAULT_WORKER_INTERVAL = 5;
    static private $QUEUE_SIGS = array(SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGCONT, SIGHUP, SIGWINCH);
    static private $CHUNK_SIZE = 16384;

    static private $appName = '';
    static private $afterPreFork;
    static private $configFiles = array('resque-pool.yml', 'config/resque-pool.yml');
    static private $termBehavior; // how to act when SIGTERM is received
    static private $workerClass = '\\Resque_Worker';
    static private $pipe = array();

    static private $waitingForReaper = false;

    private $configFile;
    private $config;

    private $workers = array();
    private $handleWinch = false;
    private $sigQueue = array();


    static public function afterPreFork($callable = null)
    {
        return $callable === null ? self::$afterPreFork : (self::$afterPreFork = $callable);
    }

    static public function configFiles($configFiles = null)
    {
        return $configFiles === null ? self::$configFiles : (self::$configFiles = $configFiles);
    }

    static public function appName($appName = null)
    {
        if ($appName !== null) {
            self::$appName = $appName;
        } elseif (self::$appName === null) {
            self::$appName = basename(getcwd());
        }

        return self::$appName;
    }

    static public function handleWinch($bool = null)
    {
        return $bool === null ? self::$handleWinch : (self::$handleWinch = !!$bool);
    }

    static public function termBehavior($behavior = null)
    {
        return $behavior === null ? self::$termBehavior : (self::$termBehavior = $behavior);
    }

    static public function workerClass($class = null)
    {
        return $class === null ? self::$workerClass : (self::$workerClass = $class);
    }

    static public function chooseConfigFile()
    {
        if ($chosen = getenv('RESQUE_POOL_CONFIG')) {
            return $chosen;
        }
        foreach (self::$configFiles as $file) {
            if (file_exists($file)) {
                return $file;
            }
        }

        return null;
    }

    static public function run()
    {
        $instance = new self(self::chooseConfigFile());
        $instance->start()->join();
    }

    public function __construct($config, Logger $logger = null)
    {
        $this->logger = $logger ? $logger : new Logger;
        $this->initConfig($config);
        $this->logger->procline('(initialized)');
    }

    public function start()
    {
        declare(ticks = 1);
        $this->logger->procline('(starting)');
        $this->initSelfPipe();
        $this->initSigHandlers();
        $this->maintainWorkerCount();
        $this->logger->procline('(started)');
        $this->logger->log("started manager");
        $this->reportWorkerPoolPids();

        return $this;
    }

    public function reportWorkerPoolPids()
    {
        if (count($this->workers) === 0) {
            $this->logger->log('Pool is empty');
        } else {
            $pids = $this->allPids();
            $this->logger->log("Pool contains worker PIDs: ".implode(', ', $pids));
        }
    }

    public function join()
    {
        while(true) {
            $this->reapAllWorkers();
            if ($this->handleSignalQueue()) {
                break;
            }
            if (0 === count($this->sigQueue)) {
                $this->masterSleep();
                $this->maintainWorkerCount();
            }
            $this->logger->procline(sprintf("managing [%s]", implode(' ', $this->allPids())));
        }
        $this->logger->procline("(shutting down)");
        $this->logger->log('manager finished');
    }

    public function config($key = null)
    {
        if ($key === null) {
            return $this->config;
        }

        return isset($this->config[$key]) ? $this->config[$key] : null;
    }

    protected function initConfig($config)
    {
        if ($config === null || is_string($config)) {
            $this->configFile = $config;
        } else {
            $this->config = $config;
        }
        $this->loadConfig();
    }

    protected function loadConfig()
    {
        if ($this->configFile) {
            $this->logger->log("Loading config file: {$this->configFile}");
            Yaml::enablePhpParsing();
            try {
                $this->config = Yaml::parse($this->configFile);
            } catch (ParseException $e) {
                $this->logger->log('Invalid config file: ' . $e->getMessage());
                exit(1);
            }
        } elseif (!$this->config) {
            $this->config = array();
        }
        $environment = getenv('RESQUE_ENV');
        if ($environment && isset($this->config[$environment])) {
            $this->config = $this->config[$environment] + $this->config;
        }
        // filter out the environments
        $this->config = array_filter($this->config, 'is_integer');

        $this->logger->log("Configured queues: " . implode(", ", array_keys($this->config)));
    }

    protected function initSelfPipe()
    {
        foreach (self::$pipe as $fd) {
            fclose($fd);
        }
        self::$pipe = array();
        if (false === socket_create_pair(AF_UNIX, SOCK_STREAM, 0, self::$pipe)) {
            $msg = 'socket_create_pair failed. Reason '.socket_strerror(socket_last_error());
            $this->logger->log($msg);
            die($msg);
        }
    }
    protected function initSigHandlers()
    {
        foreach (self::$QUEUE_SIGS as $signal) {
            pcntl_signal($signal, array($this, 'trapDeferred'));
        }
        pcntl_signal(SIGCHLD, array($this, 'awakenMaster'));
    }

    public function awakenMaster()
    {
        socket_write(self::$pipe[0], '.', 1);
    }

    public function trapDeferred($signal)
    {
        if (self::$waitingForReaper && in_array($signal, array(SIGINT, SIGTERM))) {
            $this->logger->log("Recieved $signal: short circuiting QUIT waitpid");
            throw new QuitNowException();
        }
        if (count($this->sigQueue) < self::$SIG_QUEUE_MAX_SIZE) {
            $this->sigQueue[] = $signal;
        } else {
            $this->logger->log("Ignoring SIG$signal, queue=" . var_export($this->sigQueue, true));
        }
    }

    protected function resetSigHandlers()
    {
        $noop = function() {};
        foreach (self::$QUEUE_SIGS as $sig) {
            pcntl_signal($sig, $noop);
        }
        pcntl_signal(SIGCHLD, $noop);
    }

    protected function handleSignalQueue()
    {
        switch($signal = array_shift($this->sigQueue)) {
        case SIGUSR1:
        case SIGUSR2:
        case SIGCONT:
            $this->logger->log("$signal: sending to all workers");
            $this->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->log("HUP: reload config file and reload logfiles");
            $this->loadConfig();
            $this->logger->log('HUP: gracefully shutdown old children (which have old logfiles open)');
            $this->signalAllWorkers(SIGQUIT);
            $this->logger->log('HUP: new children will inherit new logfiles');
            $this->maintainWorkerCount();
            break;
        case SIGWINCH:
            if (self::$handleWinch) {
                $this->logger->log('WINCH: gracefully stopping all workers');
                $this->config = array();
                $this->maintainWorkerCount();
            }
            break;
        case SIGQUIT:
            $this->gracefulWorkerShutdownAndWait($signal);
            return true;
        case SIGINT:
            $this->gracefulWorkerShutdown($signal);
            return true;
        case SIGTERM:
            switch (self::$termBehavior) {
            case "graceful_worker_shutdown_and_wait":
                $this->gracefulWorkerShutdownAndWait($signal);
                break;
            case "graceful_worker_shutdown":
                $this->gracefulWorkerShutdown($signal);
                break;
            default:
                $this->shutdownEverythingNow($signal);
                break;
            }
            return true;
        }
    }

    protected function gracefulWorkerShutdownAndWait($signal)
    {
        $this->logger->log("$signal: graceful shutdown, waiting for children");
        $this->signalAllWorkers(SIGQUIT);
        $this->reapAllWorkers(0); // will hang until all workers are shutdown
    }

    protected function gracefulWorkerShutdown($signal)
    {
        $this->logger->log("$signal: immediate shutdown (graceful worker shutdown)");
        $this->signalAllWorkers(SIGQUIT);
    }

    protected function shutdownEverythingNow($signal)
    {
        $this->logger->log("$signal: $immediate shutdown (and immediate worker shutdown)");
        $this->signalAllWorkers(SIGTERM);
    }

    protected function masterSleep()
    {
        $read = array(self::$pipe[1]);
        $write = array();
        $except = array();
        // we need the @ to hide the interupted system call if a signal is received
        $ready = @socket_select($read, $write, $except, 1); // socket_select requires references
        if ($ready === false || $ready === 0) {
            // on error FALSE is returned and a warning raised (this can happen if the system
            // call is interrupted by an incoming signal)
            return;
        }
        socket_set_nonblock(self::$pipe[1]);
        do {
            $result = socket_read(self::$pipe[1], self::$CHUNK_SIZE, PHP_BINARY_READ);
        } while($result !== false && $result !== "");
    }

    protected function reapAllWorkers($waitpidFlags = WNOHANG)
    {
        self::$waitingForReaper = ($waitpidFlags === 0);

        while(true) {
            $wpid = pcntl_waitpid(-1, $status, $waitpidFlags);
            // 0 is WNOHANG and no dead children, -1 is no children exist
            if ($wpid === 0 || $wpid === -1) {
                break;
            }

            $exit = pcntl_wexitstatus($status);
            $this->logger->log("Reaped resque worker $wpid (status: $exit) queues: ". $this->workerQueues($wpid));
            $this->deleteWorker($wpid);
        }
    }

    public function workerQueues($pid)
    {
        foreach ($this->workers as $queues => $workers) {
            if (isset($workers[$pid])) {

                return $queues;
            }
        }

        return false;
    }

    protected function deleteWorker($pid)
    {
        foreach (array_keys($this->workers) as $queues) {
            if (isset($this->workers[$queues][$pid])) {
                unset($this->workers[$queues][$pid]);

                return ;
            }
        }
    }

    public function allPids()
    {
        if (!$this->workers) {
            return array();
        }

        $result = array();
        foreach ($this->workers as $queues) {
            $result[] = array_keys($queues);
        }

        return call_user_func_array('array_merge', array_map(function($x) { return array_keys($x); }, $this->workers));
    }

    protected function signalAllWorkers($signal)
    {
        foreach($this->allPids() as $pid) {
            posix_kill($pid, $signal);
        }
    }

    protected function maintainWorkerCount()
    {
        foreach ($this->allKnownQueues() as $queues) {
            $delta = $this->workerDeltaFor($queues);
            if ($delta > 0) {
                $this->spawnMissingWorkersFor($queues);
            } elseif ($delta < 0) {
                $this->quitExcessWorkersFor($queues);
            }
        }
    }

    public function allKnownQueues()
    {
        return array_merge(array_keys($this->config), array_keys($this->workers));
    }

    protected function spawnMissingWorkersFor($queues)
    {
        $delta = $this->workerDeltaFor($queues);
        while($delta-- > 0) {
            $this->spawnWorker($queues);
        }
    }

    protected function quitExcessWorkersFor($queues)
    {
        $delta = -$this->workerDeltaFor($queues);
        foreach(array_slice($this->pidsFor($queues), 0, $delta) as $pid) {
            posix_kill($pid, SIGQUIT);
        }
    }

    protected function workerDeltaFor($queues)
    {
        $max = isset($this->config[$queues]) ? $this->config[$queues] : 0;
        $active = isset($this->workers[$queues]) ? count($this->workers[$queues]) : 0;

        return $max - $active;
    }

    protected function pidsFor($queues)
    {
        return isset($this->workers[$queues]) ? array_keys($this->workers[$queues]) : array();
    }

    /**
     * NOTE: the only time resque code is ever loaded is *after* this fork.
     *       this way resque(and application) code is loaded per fork and
     *       will pick up changed files.
     */
    protected function spawnWorker($queues)
    {
        $pid = pcntl_fork();
        if ($pid === -1) {
            die('could not fork');
        } elseif($pid === 0) {
            $worker = $this->createWorker($queues);
            $this->logger->logWorker("Starting worker $worker");
            $this->logger->procline("Starting worker $worker");
            $this->callAfterPrefork();
            $this->resetSigHandlers();
            $worker->work(getenv('INTERVAL') || self::$DEFAULT_WORKER_INTERVAL);
            exit(0);
        }
        $this->workers[$queues][$pid] = true;
    }

    protected function callAfterPrefork()
    {
        ($callable = self::$afterPreFork) && $callable($this, $worker);
    }

    protected function createWorker($queues)
    {
        $queues = explode(',', $queues);
        $worker = new self::$workerClass($queues);
        if (getenv('VVERBOSE')) {
            $worker->logLevel = \Resque_Worker::LOG_VERBOSE;
        } elseif(getenv('LOGGING') || getenv('VERBOSE')) {
            $worker->logLevel = \Resque_Worker::LOG_NORMAL;
        }

        return $worker;
    }
}
