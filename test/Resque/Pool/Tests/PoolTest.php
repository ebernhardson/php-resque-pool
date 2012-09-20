<?php

namespace Resque\Pool\Tests;

use Resque\Pool\Configuration;
use Resque\Pool\Logger;
use Resque\Pool\Pool;

class PoolTest extends BaseTestCase
{
    public function testInstantiate()
    {
        $config = new Configuration;
        $logger = new Logger;
        $pool = new Pool($config, $logger);
        $this->assertSame($config, $pool->getConfiguration());
        $this->assertSame($logger, $pool->getLogger());
    }

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
        list($pool, $pids) = $this->poolForSpawn(array('foo'=>1,'bar,baz'=>3));
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
        $pool->getConfiguration()->resetQueues();
        $this->assertEquals(array(), $pool->allKnownQueues());

        list($pool, $pids) = $this->poolForSpawn(array('foo'=>1,'bar,baz'=>3));
        $pool->maintainWorkerCount();
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
        $pool->getConfiguration()->resetQueues();
        // These queues are still known because they haven't been reaped yet
        $this->assertArrayEquals(array('foo','bar,baz'), $pool->allKnownQueues());
    }

    public function testWorkerQueues()
    {
        list($pool, $pids) = $this->poolForSpawn(array('baz'=>1));
        $this->assertFalse($pool->workerQueues(null));
        $this->assertFalse($pool->workerQueues(array()));
        $this->assertFalse($pool->workerQueues('foo'));

        $this->assertFalse($pool->workerQueues(reset($pids)));
        $pool->maintainWorkerCount();
        $this->assertEquals('baz', $pool->workerQueues(reset($pids)));
    }

    public function testCallAfterPreFork()
    {
        $config = new Configuration(array('bang,boom'=>1));
        $config->workerClass = __NAMESPACE__.'\\Mock\\Worker';
        $config->spawnWorker = function() {
            return 0; // 0 means pretend its the
        };
        $config->endWorker = function() {};

        $pool = new Pool($config, $this->mockLogger());
        $called = 0;
        $test = $this;
        $config->afterPreFork = function($pool, $worker) use(&$called, $test) {
            $test->assertInstanceOf('Resque\\Pool\\Pool', $pool);
            $test->assertInstanceOf(__NAMESPACE__.'\\Mock\\Worker', $worker);
            $called++;
        };

        $pool->maintainWorkerCount();
        $this->assertEquals(1, $called);
        $this->assertEquals(1, Mock\Worker::$instances[0]->calledWork);
        $this->assertArrayEquals(array('bang','boom'), Mock\Worker::$instances[0]->queues);
    }

    protected function poolForSpawn($queueConfig = null)
    {
        $config = new Configuration($queueConfig);
        $pool = new Pool($config, $this->mockLogger());

        $workers = array_sum($queueConfig);
        $pids = range(1,$workers);
        $config->spawnWorker = function() use (&$pids) {
            return array_shift($pids);
        };

        return array($pool, $pids); // NOTE: the returned $pids is a copy, not a reference like in the closure
    }
}
