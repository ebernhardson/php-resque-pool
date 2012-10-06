<?php

namespace Resque\Pool;

/**
 * Worker Pool for php-resque-pool
 *
 * @package   Resque-Pool
 * @auther    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Pool
{
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

    /**
     * @param bool
     */
    private $waitingForReaper = false;

    public function __construct(Configuration $config)
    {
        $this->config = $config;
        $this->logger = $config->logger;
        $this->platform = $config->platform;
    }

    public function getConfiguration()
    {
        return $this->config;
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

    public function waitingForReaper()
    {
        return $this->waitingForReaper;
    }

    public function reapAllWorkers($wait = false)
    {
        while ($exited = $this->platform->nextDeadChild($wait)) {
            list($wpid, $exit) = $exited;
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
        $this->logger->log("$signal: graceful shutdown, waiting for children");
        $this->signalAllWorkers(SIGQUIT);
        $this->reapAllWorkers(true); // will hang until all workers are shutdown
    }

    public function gracefulWorkerShutdown($signal)
    {
        $this->logger->log("$signal: immediate shutdown (graceful worker shutdown)");
        $this->signalAllWorkers(SIGQUIT);
    }

    public function shutdownEverythingNow($signal)
    {
        $this->logger->log("$signal: $immediate shutdown (and immediate worker shutdown)");
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
     *       code pre-fork so that the copy-on-write functionality can share the compiled
     *       code between workers.  Some investigation must be done here.
     */
    protected function spawnWorker($queues)
    {
        $pid = $this->platform->pcntl_fork();
        if ($pid === 0) {
            $this->platform->releaseSignals();
            $worker = $this->createWorker($queues);
            $this->logger->logWorker("Starting worker $worker");
            $this->logger->procline("Starting worker $worker");
            $this->callAfterPrefork($worker);
            $worker->work($this->config->workerInterval);
            $this->logger->logWorker("Worker returned from work: ".$this->config->workerInterval);
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
