<?php

namespace Resque\Pool\Test;

use Resque\Pool\Pool;

class PoolTest extends \PHPUnit_Framework_TestCase
{
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
                $test->assertEquals(1, $subject->config('foo'), $msg);
                $test->assertEquals(2, $subject->config('bar'), $msg);
                $test->assertEquals(3, $subject->config('foo,bar'), $msg);
                $test->assertEquals(4, $subject->config('bar,foo'), $msg);
                $test->assertArrayEquals(array('foo', 'bar', 'foo,bar', 'bar,foo'), $subject->allKnownQueues(), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $test->assertEquals(8, $subject->config('foo'), 'should load the default values from the Hash');
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'Only queues should be returned from allKnownQueues';
                $test->assertArrayEquals(array('foo', 'bar', 'foo,bar'), $subject->allKnownQueues(), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'When RESQUE_ENV is set it should merge the values for the correct RESQUE_ENV';
                $test->assertEquals(10, $subject->config('bar'), $msg);
                $test->assertEquals(12, $subject->config('foo,bar'), $msg);
            }),
            array($config, $testEnv, function($test, $subject) {
                $msg = 'When RESQUE_ENV is set it should not load the values for the other environments';
                $test->assertEquals(12, $subject->config('foo,bar'), $msg);
                $test->assertNull($subject->config('baz'), $msg);
            }),
            array($config, $devEnv, function($test, $subject) {
                $msg = "When RESQUE_ENV is set it should load the config for that environment";
                $test->assertEquals(8, $subject->config('foo'), $msg);
                $test->assertEquals(16, $subject->config('foo,bar'), $msg);
                $test->assertEquals(14, $subject->config('baz'), $msg);
                $test->assertNull($subject->config('bar'), $msg);
            }),
            array($config, $noEnv, function($test, $subject) {
                $msg = 'when there is no environment it should load the default values only';
                $test->assertEquals(8, $subject->config('foo'), $msg);
                $test->assertNull($subject->config('bar'), $msg);
                $test->assertNull($subject->config('foo,bar'), $msg);
                $test->assertNull($subject->config('baz'), $msg);
            }),
            array(null, null, function($test, $subject) {
                $test->assertEquals(array(), $subject->config(), 'given no configuration it should have no worker types');
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $test->assertEquals(1, $subject->config('foo'), "when RESQUE_ENV is set it should load the default YAML");
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $msg = 'when RAILS_ENV is set it should merge the YAML for the correct RESQUE_ENV';
                $test->assertEquals(5, $subject->config('bar'), $msg);
                $test->assertEquals(3, $subject->config('foo,bar'), $msg);
            }),
            array($configFile, $testEnv, function($test, $subject) {
                $msg = 'when RAILS_ENV is set it should not load the YAML for the other environments';
                $test->assertEquals(1, $subject->config('foo'), $msg);
                $test->assertEquals(5, $subject->config('bar'), $msg);
                $test->assertEquals(3, $subject->config('foo,bar'), $msg);
                $test->assertNull($subject->config('baz'), $msg);
            }),
            array($configFile, $devEnv, function($test, $subject) {
                $msg = "When RESQUE_ENV is set it should load the config for that environment";
                $test->assertEquals(1, $subject->config('foo'), $msg);
                $test->assertEquals(4, $subject->config('foo,bar'), $msg);
                $test->assertEquals(23, $subject->config('baz'), $msg);
                $test->assertNull($subject->config('bar'), $msg);
            }),
            array($configFile, $noEnv, function($test, $subject) {
                $msg = 'when there is no environment it should load the default values only';
                $test->assertEquals(1, $subject->config('foo'), $msg);
                $test->assertNull($subject->config('bar'), $msg);
                $test->assertNull($subject->config('foo,bar'), $msg);
                $test->assertNull($subject->config('baz'), $msg);
            }),
            array($customConfigFile, $noEnv, function($test, $subject) {
                $test->assertEquals(2, $subject->config('foo'), 'when there is php in the yaml it should be parsed');
            }),
        );

    }

    /**
     * @dataProvider loadingThePoolConfigurationProvider
     */
    public function testLoadingThePoolConfiguration($config, $before, $test)
    {
        $before && $before();

        $test($this, new Pool($config, $this->getMock('Resque\\Pool\\Logger')));
    }

    public function assertArrayEquals($expect, $subject)
    {
        $this->assertInternalType('array', $expect);
        $this->assertInternalType('array', $subject);
        $this->assertEquals(count($expect), count($subject));
        foreach ($expect as $item) {
            $this->assertContains($item, $subject);
        }
    }
}
