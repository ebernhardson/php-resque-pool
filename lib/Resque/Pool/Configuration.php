<?php

namespace Resque\Pool;

use Symfony\Component\Yaml\Yaml,
    Symfony\Component\Yaml\Exception\ParseException;

class Configuration
{
    const LOG_NONE = 0;
    const LOG_NORMAL = 1;
    const LOG_VERBOSE = 2;

    public $appName;
    public $afterPreFork;
    public $configFiles = array('resque-pool.yml', 'config/resque-pool.yml');
    public $logLevel = self::LOG_NONE;
    public $termBehavior;
    public $workerClass = '\\Resque_Worker';
    public $workerInterval;

    public $queueConfigFile;
    public $queueConfig;

    public $handleWinch = false;

    public function __construct($config = null)
    {
        $this->appName = basename(getcwd());
        $this->environment = getenv('RESQUE_ENV');
        $this->workerInterval = getenv('INTERVAL') ?: null;

        if (getenv('VVERBOSE')) {
            $this->logLevel = self::LOG_VERBOSE;
        } elseif (getenv('LOGGING') || getenv('VERBOSE')) {
            $this->logLevel = self::LOG_NORMAL;
        }

        if (is_array($config)) {
            $this->queueConfig = $config;
        } elseif (is_string($config)) {
            $this->queueConfigFile  = $config;
        } elseif ($config !== null) {
            throw new \InvalidArgumentException('Unknown $config argument passed');
        }
    }

    public function chooseConfigFile()
    {
        if ($chosen = getenv('RESQUE_POOL_CONFIG')) {
            return $chosen;
        }
        foreach ($this->configFiles as $file) {
            if (file_exists($file)) {
                echo "chose $file\n";
                return $file;
            }
        }

        return null;
    }

    public function initialize(Logger $logger = null)
    {
        if ( ! $this->queueConfig) {
            if ( ! $this->queueConfigFile) {
                $this->queueConfigFile = $this->chooseConfigFile();
            }

            $this->loadQueueConfig($logger);
        }
        if ($this->environment && isset($this->queueConfig[$this->environment])) {
            $this->queueConfig = $this->queueConfig[$this->environment] + $this->queueConfig;
        }
        // filter out the environments
        $this->queueConfig = array_filter($this->queueConfig, 'is_integer');
        $logger && $logger->log("Configured queues: " . implode(", ", array_keys($this->queueConfig)));
    }

    public function loadQueueConfig(Logger $logger = null)
    {
        if ($this->queueConfigFile && !file_exists($this->queueConfigFile)) {
            $logger && $logger->log("Chosen config file '{$this->queueConfigFile} not found.  Looking for others.");
            $this->queueConfigFile = $this->chooseConfigFile();
        }
        if ($this->queueConfigFile) {
            $logger && $logger->log("Loading config file: {$this->queueConfigFile}");
            Yaml::enablePhpParsing();
            try {
                $this->queueConfig = Yaml::parse($this->queueConfigFile);
            } catch (ParseException $e) {
                if ($logger !== null) {
                    $logger->log("Invalid config file: ".$e->getMessage());
                    exit(1);
                }

                throw $e;
            }
        } elseif (!$this->queueConfig) {
            $this->queueConfig = array();
        }
    }

    /**
     * @param string $queues
     * @return integer
     */
    public function workerCount($queues)
    {
        return isset($this->queueConfig[$queues]) ? $this->queueConfig[$queues] : 0;
    }

    public function knownQueues()
    {
        return $this->queueConfig ? array_keys($this->queueConfig) : array();
    }
}
