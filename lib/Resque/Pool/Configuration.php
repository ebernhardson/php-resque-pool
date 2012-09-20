<?php

namespace Resque\Pool;

use Symfony\Component\Yaml\Yaml,
    Symfony\Component\Yaml\Exception\ParseException;

/**
 * Configuration manager for php-resque-pool
 *
 * @package   Resque-Pool
 * @auther    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Configuration
{
    const LOG_NONE = 0;
    const LOG_NORMAL = 1;
    const LOG_VERBOSE = 2;

    const DEFAULT_WORKER_INTERVAL = 5;

    public $afterPreFork;
    public $appName;
    public $configFiles = array('resque-pool.yml', 'config/resque-pool.yml');
    public $handleWinch = false;
    public $logLevel = self::LOG_NONE;
    public $queueConfigFile;
    public $termBehavior;
    public $waitingForReaper = false;
    public $workerClass = '\\Resque_Worker';
    public $workerInterval = self::DEFAULT_WORKER_INTERVAL;

    // callbacks used to handle fork/exit under test. additionally allows
    // PoolManager to reset the signal handlers when workers fork.
    public $endWorker;
    public $spawnWorker;

    protected $queueConfig;

    /**
     * @param array|string|null $config Either a configuration array, path to yml
     *                                  file containing config, or null
     * @param Logger|null       $logger If not provided one will be instantiated
     */
    public function __construct($config = null, Logger $logger = null)
    {
        $this->logger = $logger ?: new Logger;
        $this->loadEnvironment();

        if (is_array($config)) {
            $this->queueConfig = $config;
            $this->queueConfigFile = null;
        } elseif (is_string($config)) {
            $this->queueConfigFile  = $config;
        } elseif ($config !== null) {
            throw new \InvalidArgumentException('Unknown $config argument passed');
        }
    }

    public function initialize()
    {
        if (!$this->queueConfig) {
            $this->chooseConfigFile();
            $this->loadQueueConfig();
        }
        if ($this->environment && isset($this->queueConfig[$this->environment])) {
            $this->queueConfig = $this->queueConfig[$this->environment] + $this->queueConfig;
        }
        // filter out the environments
        $this->queueConfig = array_filter($this->queueConfig, 'is_integer');
        $this->logger->log("Configured queues: " . implode(" ", $this->knownQueues()));
    }

    /**
     * @param string $queues
     *
     * @return integer Desired number of workers for specified queue combination
     */
    public function workerCount($queues)
    {
        return isset($this->queueConfig[$queues]) ? $this->queueConfig[$queues] : 0;
    }

    /**
     * @return [string] All configured queue combinations
     */
    public function knownQueues()
    {
        return $this->queueConfig ? array_keys($this->queueConfig) : array();
    }

    /**
     * @return [string => integer] Map of queue combination to desired worker count
     */
    public function queueConfig()
    {
        return $this->queueConfig;
    }

    /**
     * Resets the current queue configuration
     */
    public function resetQueues()
    {
        $this->queueConfig = array();
    }

    protected function loadEnvironment()
    {
        $this->appName = basename(getcwd());
        $this->environment = getenv('RESQUE_ENV');
        $this->workerInterval = getenv('INTERVAL') ?: $this->workerInterval;
        $this->queueConfigFile = getenv('RESQUE_POOL_CONFIG');

        if (getenv('VVERBOSE')) {
            $this->logLevel = self::LOG_VERBOSE;
        } elseif (getenv('LOGGING') || getenv('VERBOSE')) {
            $this->logLevel = self::LOG_NORMAL;
        }
    }

    protected function chooseConfigFile()
    {
        if ($this->queueConfigFile) {
            if (file_exists($this->queueConfigFile)) {
                return $this->queueConfigFile;
            }
            $this->logger->log("Chosen config file '{$this->queueConfigFile} not found.  Looking for others.");
        }
        $this->queueConfigFile = null;
        foreach ($this->configFiles as $file) {
            if (file_exists($file)) {
                $this->queueConfigFile = $file;
                break;
            }
        }
    }

    protected function loadQueueConfig()
    {
        if ($this->queueConfigFile) {
            $this->logger->log("Loading config file: {$this->queueConfigFile}");
            Yaml::enablePhpParsing();
            try {
                $this->queueConfig = Yaml::parse($this->queueConfigFile);
            } catch (ParseException $e) {
                $msg = "Invalid config file: ".$e->getMessage();
                $this->logger->log($msg);

                throw new RuntimeException($msg, 0, $e);
            }
        }
        if (!$this->queueConfig) {
            $this->logger->log('No configuration loaded.');
            $this->queueConfig = array();
        }
    }
}
