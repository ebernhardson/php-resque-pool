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

    private $pipe = array();
    private $waitingForReaper = false;
    private $workers = array();
    private $sigQueue = array();

    static public function run($config=null, Logger $logger = null)
    {
        if (!$config instanceof Configuration) {
            $config = new Configuration($config);
        }
        $logger = $logger ?: new Logger($config->appName);
        $instance = new self($config, $logger);

        $instance->start()->join();
    }

    public function __construct(Configuration $config, Logger $logger)
    {
        $this->logger = $logger;
        $this->config = $config;
        $this->config->initialize($this->logger);
        $this->logger->procline('(initialized)');
    }

    public function start()
    {
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

    protected function initSelfPipe()
    {
        foreach ($this->pipe as $fd) {
            fclose($fd);
        }
        $this->pipe = array();
        if (false === socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $this->pipe)) {
            $msg = 'socket_create_pair failed. Reason '.socket_strerror(socket_last_error());
            $this->logger->log($msg);
            die($msg);
        }
    }

    public function awakenMaster()
    {
        socket_write($this->pipe[0], '.', 1);
    }

    protected function initSigHandlers()
    {
        foreach (self::$QUEUE_SIGS as $signal) {
            pcntl_signal($signal, array($this, 'trapDeferred'));
        }
        pcntl_signal(SIGCHLD, array($this, 'awakenMaster'));
    }

    public function trapDeferred($signal)
    {
        if ($this->waitingForReaper && in_array($signal, array(SIGINT, SIGTERM))) {
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
        // this will queue up signals into $this->sigQueue
        pcntl_signal_dispatch();

        // now process them
        switch($signal = array_shift($this->sigQueue)) {
        case SIGUSR1:
        case SIGUSR2:
        case SIGCONT:
            $this->logger->log("$signal: sending to all workers");
            $this->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->log("HUP: reload config file and reload logfiles");
            $this->config->queueConfig = array();
            $this->config->initialize($this->logger);
            $this->logger->log('HUP: gracefully shutdown old children (which have old logfiles open)');
            $this->signalAllWorkers(SIGQUIT);
            $this->logger->log('HUP: new children will inherit new logfiles');
            $this->maintainWorkerCount();
            break;
        case SIGWINCH:
            if ($this->config->handleWinch) {
                $this->logger->log('WINCH: gracefully stopping all workers');
                $this->config->queueConfig = array();
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
            switch ($this->config->termBehavior) {
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
        $read = array($this->pipe[1]);
        $write = array();
        $except = array();
        // we need the @ to hide the interupted system call if a signal is received
        $ready = @socket_select($read, $write, $except, 1); // socket_select requires references
        if ($ready === false || $ready === 0) {
            // on error FALSE is returned and a warning raised (this can happen if the system
            // call is interrupted by an incoming signal)
            return;
        }
        socket_set_nonblock($this->pipe[1]);
        do {
            $result = socket_read($this->pipe[1], self::$CHUNK_SIZE, PHP_BINARY_READ);
        } while($result !== false && $result !== "");
    }

    protected function reapAllWorkers($waitpidFlags = WNOHANG)
    {
        $this->waitingForReaper = ($waitpidFlags === 0);

        while(true) {
            pcntl_signal_dispatch();

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
        return array_merge($this->config->knownQueues(), array_keys($this->workers));
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
        $max = $this->config->workerCount($queues);
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
            $msg = 'Fatal Error: pcntl_fork returned -1';
            $this->logger->log($msg);
            die($msg);
        } elseif($pid === 0) {
            $worker = $this->createWorker($queues);
            $this->logger->logWorker("Starting worker $worker");
            $this->logger->procline("Starting worker $worker");
            $this->callAfterPrefork();
            $this->resetSigHandlers();
            $worker->work($this->config->workerInterval || self::$DEFAULT_WORKER_INTERVAL);
            exit(0);
        }
        $this->workers[$queues][$pid] = true;
    }

    protected function callAfterPrefork()
    {
        ($callable = $this->config->afterPreFork) && $callable($this, $worker);
    }

    protected function createWorker($queues)
    {
        $queues = explode(',', $queues);
        $class = $this->config->workerClass;
        $worker = new $class($queues);
        if ($this->config->logLevel === Configuration::LOG_VERBOSE) {
            $worker->logLevel = \Resque_Worker::LOG_VERBOSE;
        } elseif($this->config->logLevel === Configuration::LOG_NORMAL) {
            $worker->logLevel = \Resque_Worker::LOG_NORMAL;
        }

        return $worker;
    }
}
