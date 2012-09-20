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

    public function __construct(Configuration $config, Logger $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
    }

    public function getConfiguration()
    {
        return $this->config;
    }

    public function getLogger()
    {
        return $this->logger;
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

    public function gracefulWorkerShutdownAndWait($signal)
    {
        $this->logger->log("$signal: graceful shutdown, waiting for children");
        $this->signalAllWorkers(SIGQUIT);
        $this->reapAllWorkers(0); // will hang until all workers are shutdown
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

    public function reapAllWorkers($waitpidFlags = WNOHANG)
    {
        $this->config->waitingForReaper = ($waitpidFlags === 0);

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

    public function allKnownQueues()
    {
        return array_unique(array_merge($this->config->knownQueues(), array_keys($this->workers)));
    }

    public function signalAllWorkers($signal)
    {
        foreach($this->allPids() as $pid) {
            posix_kill($pid, $signal);
        }
    }

    public function maintainWorkerCount()
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

    protected function deleteWorker($pid)
    {
        foreach (array_keys($this->workers) as $queues) {
            if (isset($this->workers[$queues][$pid])) {
                unset($this->workers[$queues][$pid]);

                return ;
            }
        }
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
     * TODO: the other possibility here is to load all the resque(and possibly application)
     *       code pre-fork so that the copy-on-write functionality can share the compiled
     *       code between workers.  Some investigation must be done here.
     */
    protected function spawnWorker($queues)
    {
        $spawnWorker = $this->config->spawnWorker;
        $pid = call_user_func($spawnWorker);
        if($pid === 0) {
            $worker = $this->createWorker($queues);
            $this->logger->logWorker("Starting worker $worker");
            $this->logger->procline("Starting worker $worker");
            $this->callAfterPrefork($worker);
            $worker->work($this->config->workerInterval);
            $this->logger->logWorker("Worker returned from work: ".$this->config->workerInterval);

            $endWorker = $this->config->endWorker;
            call_user_func($endWorker);
        } else {
            $this->workers[$queues][$pid] = true;
        }
    }

    protected function callAfterPrefork($worker)
    {
        ($callable = $this->config->afterPreFork) && call_user_func($callable, $this, $worker);
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
