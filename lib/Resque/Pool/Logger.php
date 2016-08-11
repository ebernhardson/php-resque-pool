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
    private $appName;

    public function __construct($appName = null)
    {
        $this->appName = $appName ? "[{$appName}]" : "";
    }

    public function procline($string)
    {
        if (function_exists('setproctitle')) {
            setproctitle("resque-pool-manager{$this->appName}: {$string}");
        } elseif (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title("resque-pool-manager{$this->appName}: {$string}");
        }
    }

    public function log($message)
    {
        $pid = getmypid();
        echo "resque-pool-manager{$this->appName}[{$pid}]: {$message}\n";
    }

    public function logWorker($message)
    {
        $pid = getmypid();
        echo "resque-pool-worker{$this->appName}[{$pid}]: {$message}\n";
    }

    /**
     * This function closes and re-opens the output log
     */
    public function rotate()
    {
        // not possible in php?
    }
}
