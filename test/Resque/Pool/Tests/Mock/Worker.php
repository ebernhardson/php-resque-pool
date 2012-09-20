<?php

namespace Resque\Pool\Tests\Mock;

class Worker
{
    static public $instances = array();

    public $queues;
    public $calledWork = 0;

    public function __construct($queues)
    {
        self::$instances[] = $this;
        $this->queues = $queues;
    }

    public function work()
    {
        ++$this->calledWork;
    }

    public function __toString()
    {
        return __CLASS__;
    }
}
