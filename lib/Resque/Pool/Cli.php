<?php

namespace Resque\Pool;

/**
 * CLI runner for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Cli
{

    /**
     * @var array<string,array<array-key,string|bool>>
     */
    private static $optionDefs = array(
        'help' => array('Show usage information', 'default' => false, 'short' => 'h'),
        'config' => array('Alternate path to config file', 'short' => 'c'),
        'appName' => array('Alternate appname', 'short' => 'a'),
        'daemon' => array('Run as a background daemon', 'default' => false, 'short' => 'd'),
        'pidfile' => array('PID file location', 'short' => 'p'),
        'environment' => array('Set RESQUE_ENV', 'short' => 'E'),
        'term-graceful-wait' => array('On TERM signal, wait for workers to shut down gracefully'),
        'term-graceful' => array('On TERM signal, shut down workers gracefully'),
        'term_immediate' => array('On TERM signal, shut down workers immediately (default)'),
    );

    /**
     * @return void
     */
    public function run()
    {
        $opts = $this->parseOptions();

        if ($opts['daemon']) {
            $this->daemonize();
        }
        $this->managePidfile($opts['pidfile']);
        $config = $this->buildConfiguration($opts);
        $this->startPool($config);
    }

    /**
     * @return array<string,mixed>
     * @phpstan-return array{
     *   help: bool, config: string, appName: string, daemon: bool, pidfile: string,
     *   environment: string, term-graceful-wait: string, term-graceful: string, term_immediate: string
     * }
     */
    public function parseOptions()
    {
        $shortopts = '';
        $longopts = array();
        /** @var array<string,string|bool> $defaults */
        $defaults = array();
        $shortmap = array();

        foreach (self::$optionDefs as $name => $def) {
            $def += array('default' => '', 'short' => false);

            $defaults[$name] = $def['default'];
            $postfix = is_bool($defaults[$name]) ? '' : ':';

            $longopts[] = $name.$postfix;
            if ($def['short']) {
                $shortmap[$def['short']] = $name;
                $shortopts .= $def['short'].$postfix;
            }
        }

        /** @var array<string,string|bool> $received */
        $received = getopt($shortopts, $longopts);

        foreach (array_keys($received) as $key) {
            if (strlen($key) === 1) {
                $received[$shortmap[$key]] = $received[$key];
                unset($received[$key]);
            }
        }

        // getopt is odd ... it returns false for received args with no options allowed
        foreach (array_keys($received) as $key) {
            if (false === $received[$key]) {
                $received[$key] = true;
            }
        }

        $received += $defaults;

        if ($received['help']) {
            $this->usage();
            exit(0);
        }

        return $received; // @phpstan-ignore-line
    }

    /**
     * @return void
     */
    public function usage()
    {
        $cmdname = isset($GLOBALS['argv'][0]) ? $GLOBALS['argv'][0] : 'resque-pool';
        echo "\n"
            ."Usage:"
            ."\t$cmdname [OPTION]...\n"
            ."\n";
        foreach (self::$optionDefs as $name => $def) {
            $def += array('default' => '', 'short' => false);
            printf(" %2s %-20s %s\n",
                $def['short'] ? ('-'.$def['short']) : '',
                "--{$name}",
                $def[0]
            );
        }
        echo "\n\n";
    }

    /**
     * @return void
     */
    public function daemonize()
    {
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new \RuntimeException("Failed pcntl_fork");
        }
        if ($pid) {
            // parent
            echo "Started background process: {$pid}\n\n";
            exit(0);
        }
    }

    /**
     * @param string $pidfile
     * @return void
     */
    public function managePidfile($pidfile)
    {
        if (!$pidfile) {
            return;
        }

        if (file_exists($pidfile)) {
            if ($this->processStillRunning($pidfile)) {
                throw new \Exception("Pidfile already exists at '{$pidfile}' and process is still running.");
            } else {
                unlink($pidfile);
            }
        } elseif (!is_dir($piddir = dirname($pidfile))) {
            mkdir($piddir, 0777, true);
        }

        file_put_contents($pidfile, getmypid(), LOCK_EX);
        register_shutdown_function(function() use ($pidfile) {
            if (file_exists($pidfile)) {
                @unlink($pidfile);
            }
        });
    }

    /**
     * @param string $pidfile
     * @return bool
     */
    public function processStillRunning($pidfile)
    {
        $contents = file_get_contents($pidfile);
        if (false === $contents) {
            return true;
        }

        $oldPid = (int)trim($contents);

        return posix_kill($oldPid, 0);
    }

    /**
     * @param array $options
     * @phpstan-param array{
     *   appName?: string, environment?: string, config?: string, daemon?: bool,
     *   term-graceful-wait?: string, term-graceful?: string
     * } $options
     * @return Configuration
     */
    public function buildConfiguration(array $options)
    {
        $config = new Configuration;
        if (isset($options['appName'])) {
            $config->appName = $options['appName'];
        }
        if (isset($options['environment'])) {
            $config->environment = $options['environment'];
        }
        if (isset($options['config'])) {
            $config->queueConfigFile = $options['config'];
        }
        if (isset($options['daemon'])) {
            $config->handleWinch = true;
        }
        if (isset($options['term-graceful-wait'])) {
            $config->termBehavior = 'graceful_worker_shutdown_and_wait';
        } elseif (isset($options['term-graceful'])) {
            $config->termBehavior = 'graceful_worker_shutdown';
        }

        return $config;
    }

    /**
     * @return void
     */
    public function startPool(Configuration $config)
    {
        $pool = new Pool($config);
        $pool->start();
        $pool->join();
    }
}
