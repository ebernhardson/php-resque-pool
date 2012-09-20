<?php

namespace Resque\Pool\Test;

use Resque\Pool\Configuration;
use Resque\Pool\Logger;
use Resque\Pool\Pool;

class PoolTest extends \PHPUnit_Framework_TestCase
{
    public function testInstantiate()
    {
        $pool = new Pool(new Configuration, new Logger);
    }
}
