<?php

namespace Resque\Pool;

/**
 * Logger for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Logger
{
    /** @var string */
    private $appName;

    /**
     * @param null|string $appName
     */
    public function __construct($appName = null)
    {
        $this->appName = $appName ? "[{$appName}]" : "";
    }

    /**
     * @param string $string
     * @return void
     */
    public function procline($string)
    {
        if (function_exists('setproctitle')) {
            setproctitle("resque-pool-manager{$this->appName}: {$string}");
        } elseif (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title("resque-pool-manager{$this->appName}: {$string}");
        }
    }

    /**
     * @param string $message
     * @return void
     */
    public function log($message)
    {
        $pid = getmypid();
        echo "resque-pool-manager{$this->appName}[{$pid}]: {$message}\n";
    }

    /**
     * @param string $message
     * @return void
     */
    public function logWorker($message)
    {
        $pid = getmypid();
        echo "resque-pool-worker{$this->appName}[{$pid}]: {$message}\n";
    }

    /**
     * This function closes and re-opens the output log
     * @return void
     */
    public function rotate()
    {
        // not possible in php?
    }
}
