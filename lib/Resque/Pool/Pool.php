<?php

namespace Resque\Pool;

/**
 * Worker Pool for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Pool
{
    private static $QUEUE_SIGS = array(SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGCONT, SIGHUP, SIGWINCH, SIGCHLD);

    /**
     * @param Configuration
     */
    private $config;

    /**
     * @param Logger
     */
    private $logger;

    /**
     * @param [queues => [pid => true]]
     */
    private $workers = array();

    public function __construct(Configuration $config)
    {
        $this->config = $config;
        $this->logger = $config->logger;
        $this->platform = $config->platform;
    }

    public function start()
    {
        $this->config->initialize();
        $this->logger->procline('(starting)');
        $this->platform->trapSignals(self::$QUEUE_SIGS);
        $this->maintainWorkerCount();

        $this->logger->procline('(started)');
        $this->logger->log("started manager");
        $this->reportWorkerPoolPids();
    }

    public function join()
    {
        while (true) {
            $this->reapAllWorkers();
            if ($this->handleSignalQueue()) {
                break;
            }

            if (0 === $this->platform->numSignalsPending()) {
                $this->maintainWorkerCount();
                $this->platform->sleep($this->config->sleepTime);
            }

            $this->logger->procline(sprintf("managing [%s]", implode(' ', $this->allPids())));
        }
        $this->logger->procline("(shutting down)");
        $this->logger->log("manager finished");
    }

    /**
     * @return bool When true the pool manager must shut down
     */
    protected function handleSignalQueue()
    {
        switch ($signal = $this->platform->nextSignal()) {
        case SIGUSR1:
        case SIGUSR2:
        case SIGCONT:
            $this->logger->log("{$signal}: sending to all workers");
            $this->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->log("HUP: reload config file");
            $this->config->resetQueues();
            $this->config->initialize();
            $this->logger->log('HUP: gracefully shutdown old children (which have old logfiles open)');
            $this->signalAllWorkers(SIGQUIT);
            $this->logger->log('HUP: new children will inherit new logfiles');
            $this->maintainWorkerCount();
            break;
        case SIGWINCH:
            if ($this->config->handleWinch) {
                $this->logger->log('WINCH: gracefully stopping all workers');
                $this->config->resetQueues();
                $this->maintainWorkerCount();
            }
            break;
        case SIGQUIT:
            $this->platform->setQuitOnExitSignal(true);
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

        return false;
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

    /**
     * Creates or shuts down workers to match the configured worker counts.
     */
    public function maintainWorkerCount()
    {
        foreach ($this->allKnownQueues() as $queues) {
            $delta = $this->workerDeltaFor($queues);
            if ($delta > 0) {
                while ($delta-- > 0) {
                    $this->spawnWorker($queues);
                }
            } elseif ($delta < 0) {
                $pids = array_slice($this->pidsFor($queues), 0, -$delta);
                $this->platform->signalPids($pids, SIGQUIT);
            }
        }
    }

    /**
     * Finds and unsets dead workers.
     *
     * @param boolean $wait When true waits for all children to shutdown.
     */
    public function reapAllWorkers($wait = false)
    {
        while ($exited = $this->platform->nextDeadChild($wait)) {
            list($wpid, $exit) = $exited;
            $this->logger->log("Reaped resque worker {$wpid} (status: {$exit}) queues: ". $this->workerQueues($wpid));
            $this->deleteWorker($wpid);
        }
    }

    /**
     * @return string|null The queues $pid was created to work on
     */
    public function workerQueues($pid)
    {
        foreach ($this->workers as $queues => $workers) {
            if (isset($workers[$pid])) {
                return $queues;
            }
        }

        return null;
    }

    /**
     * @return [integer] The pids of all living worker daemons
     */
    public function allPids()
    {
        if (!$this->workers) {
            return array();
        }

        $result = array();
        foreach ($this->workers as $queues) {
            $result[] = array_keys($queues);
        }

        return call_user_func_array('array_merge', $result);
    }

    public function allKnownQueues()
    {
        return array_unique(array_merge($this->config->knownQueues(), array_keys($this->workers)));
    }

    public function signalAllWorkers($signal)
    {
        $this->platform->signalPids($this->allPids(), $signal);
    }

    public function gracefulWorkerShutdownAndWait($signal)
    {
        $this->logger->log("{$signal}: graceful shutdown, waiting for children");
        $this->signalAllWorkers(SIGQUIT);
        $this->reapAllWorkers(true); // will hang until all workers are shutdown
    }

    public function gracefulWorkerShutdown($signal)
    {
        $this->logger->log("{$signal}: immediate shutdown (graceful worker shutdown)");
        $this->signalAllWorkers(SIGQUIT);
    }

    public function shutdownEverythingNow($signal)
    {
        $this->logger->log("{$signal}: {$immediate} shutdown (and immediate worker shutdown)");
        $this->signalAllWorkers(SIGTERM);
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

    protected function deleteWorker($pid)
    {
        foreach (array_keys($this->workers) as $queues) {
            if (isset($this->workers[$queues][$pid])) {
                unset($this->workers[$queues][$pid]);

                return ;
            }
        }
    }

    /**
     * NOTE: the only time resque code is ever loaded is *after* this fork.
     *       this way resque(and application) code is loaded per fork and
     *       will pick up changed files.
     * TODO: the other possibility here is to load all the resque(and possibly application)
     *       code pre-fork so that the copy-on-write functionality of the linux memory model
     *       can share the compiled code between workers.  Some investigation into the facts
     *       would be usefull
     */
    protected function spawnWorker($queues)
    {
        $pid = $this->platform->pcntl_fork();
        if ($pid === -1) {
            $this->logger->log('pcntl_fork failed');
            $this->platform->exit(1);
        } elseif ($pid === 0) {
            $this->platform->releaseSignals();
            $worker = $this->createWorker($queues);
            $this->logger->logWorker("Starting worker {$worker}");
            $this->logger->procline("Starting worker {$worker}");
            $this->callAfterPrefork($worker);
            $worker->work($this->config->workerInterval);
            $this->platform->_exit(0);
        } else {
            $this->workers[$queues][$pid] = true;
        }
    }

    protected function callAfterPrefork($worker)
    {
        if ($callable = $this->config->afterPreFork) {
            call_user_func($callable, $this, $worker);
        }
    }

    protected function createWorker($queues)
    {
        $queues = explode(',', $queues);
        $class = $this->config->workerClass;
        $worker = new $class($queues);
        if ($this->config->logLevel === Configuration::LOG_VERBOSE) {
            $worker->logLevel = \Resque_Worker::LOG_VERBOSE;
        } elseif ($this->config->logLevel === Configuration::LOG_NORMAL) {
            $worker->logLevel = \Resque_Worker::LOG_NORMAL;
        }

        return $worker;
    }
}
