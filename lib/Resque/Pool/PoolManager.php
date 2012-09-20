<?php

namespace Resque\Pool;

/**
 * Pool Manager for php-resque-pool
 *
 * @package   Resque-Pool
 * @auther    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class PoolManager
{
    static private $SIG_QUEUE_MAX_SIZE = 5;
    static private $QUEUE_SIGS = array(SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGCONT, SIGHUP, SIGWINCH);
    static private $CHUNK_SIZE = 16384;

    private $pipe = array();
    private $workers = array();
    private $sigQueue = array();

    static public function run($config = null, Logger $logger = null)
    {
        if (!$config instanceof Configuration) {
            $config = new Configuration($config);
        }
        $logger = $logger ?: new Logger($config->appName);
        $instance = new self(new Pool($config, $logger));

        $instance->start()->join();
    }

    public function __construct(Pool $pool)
    {
        $this->config = $pool->getConfiguration();
        $this->logger = $pool->getLogger();
        $this->pool = $pool;

        $this->config->spawnWorker = array($this, 'spawnWorker');
        $this->config->endWorker = array($this, 'endWorker');
        $this->config->initialize($this->logger);

        $this->logger->procline('(initialized)');
    }

    public function start()
    {
        $this->logger->procline('(starting)');
        $this->initSelfPipe();
        $this->initSigHandlers();
        $this->pool->maintainWorkerCount();
        $this->logger->procline('(started)');
        $this->logger->log("started manager");
        $this->pool->reportWorkerPoolPids();

        return $this;
    }

    public function join()
    {
        while(true) {
            $this->pool->reapAllWorkers();
            if ($this->handleSignalQueue()) {
                break;
            }
            if (0 === count($this->sigQueue)) {
                $this->masterSleep();
                $this->pool->maintainWorkerCount();
            }
            $this->logger->procline(sprintf("managing [%s]", implode(' ', $this->pool->allPids())));
        }
        $this->logger->procline("(shutting down)");
        $this->logger->log('manager finished');
    }

    public function spawnWorker()
    {
        $pid = pcntl_fork();
        if ($pid === -1) {
            $msg = 'Fatal Error: pcntl_fork returned -1';
            $this->logger->log($msg);

            throw new \RuntimeException($msg);
        }
        if ($pid === 0) {
            $this->resetSigHandlers();
        }

        return $pid;
    }

    public function endWorker()
    {
        exit(0);
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
            throw new \RuntimeException($msg);
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
        if ($this->config->waitingForReaper && in_array($signal, array(SIGINT, SIGTERM))) {
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
            $this->pool->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->log("HUP: reload config file and reload logfiles");
            $this->config->resetQueues();
            $this->config->initialize($this->logger);
            $this->logger->log('HUP: gracefully shutdown old children (which have old logfiles open)');
            $this->pool->signalAllWorkers(SIGQUIT);
            $this->logger->log('HUP: new children will inherit new logfiles');
            $this->pool->maintainWorkerCount();
            break;
        case SIGWINCH:
            if ($this->config->handleWinch) {
                $this->logger->log('WINCH: gracefully stopping all workers');
                $this->config->queueConfig = array();
                $this->pool->maintainWorkerCount();
            }
            break;
        case SIGQUIT:
            $this->pool->gracefulWorkerShutdownAndWait($signal);
            return true;
        case SIGINT:
            $this->pool->gracefulWorkerShutdown($signal);
            return true;
        case SIGTERM:
            switch ($this->config->termBehavior) {
            case "graceful_worker_shutdown_and_wait":
                $this->pool->gracefulWorkerShutdownAndWait($signal);
                break;
            case "graceful_worker_shutdown":
                $this->pool->gracefulWorkerShutdown($signal);
                break;
            default:
                $this->pool->shutdownEverythingNow($signal);
                break;
            }
            return true;
        }
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
}
