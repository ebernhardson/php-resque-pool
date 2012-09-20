<?php

namespace Resque\Pool;

/**
 * CLI runner for php-resque-pool
 *
 * @package   Resque-Pool
 * @auther    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Cli
{
    static private $optionDefs = array(
        'help' => array('Show usage information', 'default' => false, 'short' => 'h'),
        'config' => array('Alternate path to config file', 'short' => 'c'),
        'appname' => array('Alternate appname', 'short' => 'a'),
        'daemon' => array('Run as a background daemon', 'default' => false, 'short' => 'd'),
        'pidfile' => array('PID file location', 'short' => 'p'),
        'environment' => array('Set RESQUE_ENV', 'short' => 'E'),
        'term-graceful-wait' => array('On TERM signal, wait for workers to shut down gracefully'),
        'term-graceful' => array('On TERM signal, shut down workers gracefully'),
        'term_immediate' => array('On TERM signal, shut down workers immediately (default)'),
    );

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

    public function parseOptions()
    {
        $shortopts = '';
        $longopts = array();
        $defaults = array();

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

        return $received;
    }

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
                "--$name",
                $def[0]
            );
        }
        echo "\n\n";
    }

    public function daemonize()
    {
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new \Exception("Failed pcntl_fork");
        }
        if ($pid) {
            // parent
            echo "Started background process: $pid\n\n";
            exit(0);
        }
    }

    public function managePidfile($pidfile)
    {
        if (!$pidfile) {
            return;
        }

        if (file_exists($pidfile)) {
            if ($this->processStillRunning($pidfile)) {
                throw new \Exception("Pidfile already exists at '$pidfile' and process is still running.");
            } else {
                unlink($pidfile);
            }
        } elseif(!is_dir($piddir = basename($pidfile))) {
            mkdir($piddir, 0777, true);
        }

        file_put_contents($pidfile, getmypid(), LOCK_EX);
        register_shutdown_function(function() use($pidfile) {
            if (getmypid() === file_get_contents($pidfile)) {
                unlink($pidfile);
            }
        });
    }

    public function processStillRunning($pidfile)
    {
        $oldPid = trim(file_get_contents($pidfile));

        return posix_kill($oldPid, 0);

    }

    public function buildConfiguration(array $options)
    {
        $config = new Configuration;
        if ($options['appname']) {
            $config->appName = $options['appName'];
        }
        if ($options['environment']) {
            putenv('RESQUE_ENV=' . $options['environment']);
        }
        if ($options['config']) {
            $config->queueConfigFile = $options['config'];
        }
        if ($options['daemon']) {
            $config->handleWinch(true);
        }
        if ($options['term-graceful-wait']) {
            $config->termBehavior = 'graceful_worker_shutdown_and_wait';
        } elseif ($options['term-graceful']) {
            $config->termBehavior = 'term_graceful';
        }

        return $config;
    }

    public function startPool(Configuration $config)
    {
        PoolManager::run($config);
    }
}
