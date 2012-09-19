<?php

namespace Resque\Pool;

class Logger
{
    private $appName;
    private $lastStatus;

    public function __construct($appName = null)
    {
        $this->appName = $appName ? "[$appName]" : "";
    }

    public function procline($string)
    {
        if(function_exists('setproctitle')) {
            setproctitle("resque-pool-manager{$this->appName}: $string");
        }
    }

    public function log($message)
    {
        $app = Pool::appName();
        $pid = getmypid();
        echo "resque-pool-manager{$this->appName}[$pid]: $message\n";
    }

    public function logWorker($message)
    {
        $app = Pool::appName();
        $pid = getmypid();
        echo "resque-pool-worker{$this->appName}[$pid]: $message\n";
    }
}
