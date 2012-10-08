<?php

namespace Resque\Pool\Tests;

use Resque\Pool\Configuration;
use Resque\Pool\Logger;
use Resque\Pool\Pool;

class PoolTest extends BaseTestCase
{
    public function maintainWorkerCountUpwardsProvider()
    {
        return array(
            array(array()),
            array(array('foo'=>1)),
            array(array('foo'=>1,'bar'=>1)),
            array(array('foo'=>7,'bar'=>5,'baz'=>3)),
        );
    }

    /**
     * @dataProvider maintainWorkerCountUpwardsProvider
     */
    public function testMaintainWorkerCountUpwards(array $queueConfig)
    {
        list($pool, $pids) = $this->poolForSpawn($queueConfig);
        $pool->maintainWorkerCount();
        $this->assertCount(array_sum($queueConfig), $pool->allPids());
    }

    public function testAllPidsEmpty()
    {
        $pool = new Pool(new Configuration, $this->mockLogger());
        $this->assertEquals(array(), $pool->allPids());
    }

    public function testAllKnownQueues()
    {
        list($pool, $config, $pids) = $this->poolForSpawn(array('foo'=>1,'bar,baz'=>3), false);
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
        $config->resetQueues();
        $this->assertEquals(array(), $pool->allKnownQueues());

        list($pool, $config, $pids) = $this->poolForSpawn(array('foo'=>1,'bar,baz'=>3));
        $pool->maintainWorkerCount();
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
        $config->resetQueues();
        // These queues are still known because they haven't been reaped yet
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
    }

    public function testWorkerQueues()
    {
        list($pool, $config, $pids) = $this->poolForSpawn(array('baz'=>1));
        $this->assertNull($pool->workerQueues(null));
        $this->assertNull($pool->workerQueues(array()));
        $this->assertNull($pool->workerQueues('foo'));

        $this->assertNull($pool->workerQueues(reset($pids)));
        $pool->maintainWorkerCount();
        $this->assertEquals('baz', $pool->workerQueues(reset($pids)));
    }

    public function testCallAfterPreFork()
    {
        $config = new Configuration(array('bang,boom'=>1));
        $config->logger = $this->mockLogger();
        $config->workerClass = __NAMESPACE__.'\\Mock\\Worker';
        $config->platform = $this->mockPlatform();
        $config->platform->expects($this->once())
            ->method('pcntl_fork')
            ->will($this->returnValue(0)); // 0 means pretend its the child process

        $pool = new Pool($config);
        $called = 0;
        $test = $this;
        $config->afterPreFork = function($pool, $worker) use (&$called, $test) {
            $test->assertInstanceOf('Resque\\Pool\\Pool', $pool);
            $test->assertInstanceOf(__NAMESPACE__.'\\Mock\\Worker', $worker);
            $called++;
        };

        $pool->maintainWorkerCount();
        $this->assertEquals(1, $called);
        $this->assertEquals(1, Mock\Worker::$instances[0]->calledWork);
        $this->assertArrayEquals(array('bang','boom'), Mock\Worker::$instances[0]->queues);
    }

    protected function poolForSpawn($queueConfig = null, $mockFork=true)
    {
        $config = new Configuration($queueConfig);
        $config->logger = $this->mockLogger();
        $config->platform = $this->mockPlatform();
        $pool = new Pool($config);

        $workers = array_sum($queueConfig);
        $pids = range(1,$workers);

        if ($mockFork) {
            $config->platform->expects($this->exactly($workers))
                ->method('pcntl_fork')
                ->will(new \PHPUnit_Framework_MockObject_Stub_ConsecutiveCalls($pids));
        }

        return array($pool, $config, $pids); // NOTE: the returned $pids is a copy, not a reference like in the closure
    }

    protected function mockPlatform()
    {
        return $this->getMock('Resque\\Pool\\Platform');
    }
}
