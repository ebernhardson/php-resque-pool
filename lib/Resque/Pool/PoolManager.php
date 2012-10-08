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
    private static $CHUNK_SIZE = 16384;
    private static $QUEUE_SIGS = array(SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGCONT, SIGHUP, SIGWINCH, SIGCHLD);

    private $config;
    private $logger;
    private $platform;
    private $pool;
    private $sigQueue = array();
    private $workers = array();

    public static function run($config = null)
    {
        if (!$config instanceof Configuration) {
            $config = new Configuration($config);
        }
        $instance = new self(new Pool($config));

        $instance->start()->join();
    }

    public function __construct(Pool $pool)
    {
        $this->config = $pool->getConfiguration();
        $this->logger = $this->config->logger;
        $this->platform = $this->config->platform;
        $this->pool = $pool;

        $this->config->initialize();

        $this->logger->procline('(initialized)');
    }

    public function start()
    {
        $this->logger->procline('(starting)');
        $this->platform->trapSignals(self::$QUEUE_SIGS);
        $this->pool->maintainWorkerCount();
        $this->logger->procline('(started)');
        $this->logger->log("started manager");
        $this->pool->reportWorkerPoolPids();

        return $this;
    }

    public function join()
    {
        while (true) {
            $this->pool->reapAllWorkers();
            if ($this->handleSignalQueue()) {
                break;
            }
            if (0 === $this->platform->numSignalsPending()) {
                $this->pool->maintainWorkerCount();
                $this->platform->sleep($this->config->sleepTime);
            }
            $this->logger->procline(sprintf("managing [%s]", implode(' ', $this->pool->allPids())));
        }
        $this->logger->procline("(shutting down)");
        $this->logger->log('manager finished');
    }

    // @return bool When true the pool manager must shut down
    protected function handleSignalQueue()
    {
        switch ($signal = $this->platform->nextSignal()) {
        case SIGUSR1:
        case SIGUSR2:
        case SIGCONT:
            $this->logger->log("$signal: sending to all workers");
            $this->pool->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->log("HUP: reload config file");
            $this->config->resetQueues();
            $this->config->initialize();
            $this->logger->log('HUP: gracefully shutdown old children (which have old logfiles open)');
            $this->pool->signalAllWorkers(SIGQUIT);
            $this->logger->log('HUP: new children will inherit new logfiles');
            $this->pool->maintainWorkerCount();
            break;
        case SIGWINCH:
            if ($this->config->handleWinch) {
                $this->logger->log('WINCH: gracefully stopping all workers');
                $this->config->resetQueues();
                $this->pool->maintainWorkerCount();
            }
            break;
        case SIGQUIT:
            $this->platform->setQuitOnExitSignal(true);
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
}
