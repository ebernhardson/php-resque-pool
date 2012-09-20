<?php

namespace Resque\Pool\Tests;

use Resque\Pool\Configuration;

class ConfigurationTest extends BaseTestCase
{
    public function testDefaultWorkerInterval()
    {
        putenv('INTERVAL=');
        $config = new Configuration(null, $this->mockLogger());
        $this->assertEquals(Configuration::DEFAULT_WORKER_INTERVAL, $config->workerInterval);
        putenv('INTERVAL=20');
        $config = new Configuration(null, $this->mockLogger());
        $this->assertEquals(20, $config->workerInterval);
    }

    public function testThrowsExceptionOnInvalidInstantiation()
    {
        $this->setExpectedException('InvalidArgumentException');
        $config = new Configuration(new \StdClass);
    }

    public function loadingThePoolConfigurationProvider()
    {
        $simpleConfig = array('foo' => 1, 'bar' => 2, 'foo,bar' => 3, "bar,foo" => 4);
        $config = array(
            'foo' => 8,
            'test'        => array('bar' => 10, 'foo,bar' => 12),
            'development' => array('baz' => 14, 'foo,bar' => 16),
        );
        $configFile = 'test/misc/resque-pool.yml';
        $customConfigFile = 'test/misc/resque-pool-custom.yml.php';

        $testEnv = function() { putenv('RESQUE_ENV=test'); };
        $devEnv  = function() { putenv('RESQUE_ENV=development'); };
        $noEnv   = function() { putenv('RESQUE_ENV='); };

        return array(
            array($simpleConfig, $noEnv, function($test, $subject) {
                $msg = 'passing a simple configuration array should load the values from the array';
                $test->assertEquals(1, $subject->workerCount('foo'), $msg);
                $test->assertEquals(2, $subject->workerCount('bar'), $msg);
                $test->assertEquals(3, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(4, $subject->workerCount('bar,foo'), $msg);
                $test->assertArrayEquals(array('foo', 'bar', 'foo,bar', 'bar,foo'), $subject->knownQueues(), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $test->assertEquals(8, $subject->workerCount('foo'), 'should load the default values from the Hash');
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'Only queues should be returned from knownQueues';
                $test->assertArrayEquals(array('foo', 'bar', 'foo,bar'), $subject->knownQueues(), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'When RESQUE_ENV is set it should merge the values for the correct RESQUE_ENV';
                $test->assertEquals(10, $subject->workerCount('bar'), $msg);
                $test->assertEquals(12, $subject->workerCount('foo,bar'), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'When RESQUE_ENV is set it should not load the values for the other environments';
                $test->assertEquals(12, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('baz'), $msg);
            }),
            array($config, $devEnv, function($test, $subject) {
                $msg = "When RESQUE_ENV is set it should load the config for that environment";
                $test->assertEquals(8, $subject->workerCount('foo'), $msg);
                $test->assertEquals(16, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(14, $subject->workerCount('baz'), $msg);
                $test->assertEquals(0, $subject->workerCount('bar'), $msg);
            }),
            array($config, $noEnv, function($test, $subject) {
                $msg = 'when there is no environment it should load the default values only';
                $test->assertEquals(8, $subject->workerCount('foo'), $msg);
                $test->assertEquals(0, $subject->workerCount('bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('baz'), $msg);
            }),
            array(array(), $noEnv, function($test, $subject) {
                $test->assertEquals(array(), $subject->queueConfig(), 'given no configuration it should have no worker types');
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $test->assertEquals(1, $subject->workerCount('foo'), "when RESQUE_ENV is set it should load the default YAML");
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $msg = 'when RAILS_ENV is set it should merge the YAML for the correct RESQUE_ENV';
                $test->assertEquals(5, $subject->workerCount('bar'), $msg);
                $test->assertEquals(3, $subject->workerCount('foo,bar'), $msg);
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $msg = 'when RAILS_ENV is set it should not load the YAML for the other environments';
                $test->assertEquals(1, $subject->workerCount('foo'), $msg);
                $test->assertEquals(5, $subject->workerCount('bar'), $msg);
                $test->assertEquals(3, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('baz'), $msg);
            }),
            array($configFile, $devEnv, function($test, $subject) {
                $msg = "When RESQUE_ENV is set it should load the config for that environment";
                $test->assertEquals(1, $subject->workerCount('foo'), $msg);
                $test->assertEquals(4, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(23, $subject->workerCount('baz'), $msg);
                $test->assertEquals(0, $subject->workerCount('bar'), $msg);
            }),
            array($configFile, $noEnv, function($test, $subject) {
                $msg = 'when there is no environment it should load the default values only';
                $test->assertEquals(1, $subject->workerCount('foo'), $msg);
                $test->assertEquals(0, $subject->workerCount('bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('foo,bar'), $msg);
                $test->assertEquals(0, $subject->workerCount('baz'), $msg);
            }),
            array($customConfigFile, $noEnv, function($test, $subject) {
                $test->assertEquals(2, $subject->workerCount('foo'), 'when there is php in the yaml it should be parsed');
            }),
        );

    }

    /**
     * @dataProvider loadingThePoolConfigurationProvider
     */
    public function testLoadingThePoolConfiguration($config, $before, $test)
    {
        $before && $before();

        $config = new Configuration($config, $this->mockLogger());
        $config->initialize();

        $test($this, $config);
    }
}
