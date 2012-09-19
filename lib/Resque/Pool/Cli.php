<?php

namespace Resque\Pool;

class Cli
{
    static private $optionDefs = array(
        'help' => array('Show usage information', 'default' => false, 'short' => 'h'),
        'config' => array('Alternate path to config file', 'short' => 'c', 'default' => 'resque-pool.yml'),
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
        $this->setupEnvironment($opts);
        $this->setPoolOptions($opts);
        $this->startPool();
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
                // getopt is odd ... it returns false for received args with no option
                $received[$shortmap[$key]] = $received[$key] === false ? true : $received[$key];
                unset($received[$key]);
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
        $cmdname = $GLOBALS['argv'][0];
        echo "\n"
            ."Usage:"
            ."\t$cmdname [OPTION]..."
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

    public function setupEnvironment(array $options)
    {
        if ($options['appname']) {
            Pool::appName($options['appname']);
        }
        if ($options['environment']) {
            putenv('RESQUE_ENVIRONMENT=' . $options['environment']);
        }
        if ($options['config']) {
            putenv('RESQUE_POOL_CONFIG=' . $options['config']);
        }
    }

    public function setPoolOptions(array $options)
    {
        if ($options['daemon']) {
            Pool::handleWinch(true);
        }
        if ($options['term-graceful-wait']) {
            Pool::termBehavior('graceful_worker_shutdown_and_wait');
        } elseif ($options['term-graceful']) {
            Pool::termBehavior('term_graceful');
        }
    }

    public function startPool()
    {
        Pool::run();
    }
}
