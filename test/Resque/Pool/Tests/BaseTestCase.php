<?php

namespace Resque\Pool\Tests;

use Resque\Pool\Configuration;

abstract class BaseTestCase extends \PHPUnit_Framework_TestCase
{
    public function assertArrayEquals($expect, $subject, $message='')
    {
        $this->assertInternalType('array', $expect, $message);
        $this->assertInternalType('array', $subject, $message);
        $this->assertEquals(count($expect), count($subject), $message);
        foreach ($expect as $item) {
            $this->assertContains($item, $subject, $message);
        }
    }

    public function mockLogger()
    {
        return $this->getMock('Resque\\Pool\\Logger');
    }
}
