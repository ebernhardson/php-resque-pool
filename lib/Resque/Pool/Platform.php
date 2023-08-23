<?php

namespace Resque\Pool;

/**
 * Platform specific funcionality of php-resque-pool.  Handles signals in/out
 * along with wrapping a few standard php functions so they can be mocked in
 * tests.
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Platform
{
    /** @var int */
    private static $SIG_QUEUE_MAX_SIZE = 5;

    /** @var null|Logger */
    protected $logger;
    /** @var bool */
    private $quitOnExitSignal;

    /** @var list<int> */
    private $sigQueue = array();
    /** @var array<int,true> */
    private $trappedSignals = array();

    /**
     * @param Logger|null $logger
     * @return void
     */
    public function setLogger(Logger $logger = null)
    {
        $this->logger = $logger;
    }

    /**
     * @param bool $bool
     * @return void
     */
    public function setQuitOnExitSignal($bool)
    {
        $this->quitOnExitSignal = $bool;
    }

    /**
     * exit is reserved word
     * @param int $status
     * @return never
     */
    public function _exit($status = 0)
    {
        exit($status);
    }

    /**
     * @return int
     */
    public function pcntl_fork()
    {
        return pcntl_fork();
    }

    /**
     * @param int $seconds
     * @return false|int
     */
    public function sleep($seconds)
    {
        return sleep($seconds);
    }

    /**
     * @param list<int>|int $pids
     * @param int $sig
     * @return void
     */
    public function signalPids($pids, $sig)
    {
        if (!is_array($pids)) {
            $pids = array($pids);
        }

        foreach ($pids as $pid) {
            posix_kill($pid, $sig);
        }
    }

    /**
     * @param list<int> $signals
     * @return void
     */
    public function trapSignals(array $signals)
    {
        foreach ($signals as $sig) {
            $this->trappedSignals[$sig] = true;
            pcntl_signal($sig, array($this, 'trapDeferred'));
        }
    }

    /**
     * @return void
     */
    public function releaseSignals()
    {
        $noop = function() {};
        foreach (array_keys($this->trappedSignals) as $sig) {
            pcntl_signal($sig, $noop);
        }

        $this->trappedSignals = array();
    }


    /**
     * called by php signal handling
     * @internal
     * @param int $signal
     * @return void
     */
    public function trapDeferred($signal)
    {
        if (count($this->sigQueue) < self::$SIG_QUEUE_MAX_SIZE) {
            if ($this->quitOnExitSignal && in_array($signal, array(SIGINT, SIGTERM))) {
                $this->log("Received {$signal}: short circuiting QUIT waitpid");
                $this->_exit(1); // TODO: should this return a failed exit code?
            }

            $this->sigQueue[] = $signal;
        } else {
            $this->log("Ignoring SIG{$signal}, queue=" . json_encode($this->sigQueue, true)); // @phpstan-ignore-line
        }
    }

    /**
     * @return int
     */
    public function numSignalsPending()
    {
        pcntl_signal_dispatch();

        return count($this->sigQueue);
    }

    /**
     * @return int|null
     */
    public function nextSignal()
    {
        // this will queue up signals into $this->sigQueue
        pcntl_signal_dispatch();

        return array_shift($this->sigQueue);
    }

    /**
     * @param  bool            $wait When non-false and there are no dead children, wait for the next one
     * @return array<int,int>|null Returns either the pid and exit code of a dead child process, or null
     */
    public function nextDeadChild($wait = false)
    {
        $wpid = pcntl_waitpid(-1, $status, $wait === false ? WNOHANG : 0);
        // 0 is WNOHANG and no dead children, -1 is no children exist
        if ($wpid === 0 || $wpid === -1) {
            return null;
        }

        /** @var int $exit */
        $exit = pcntl_wexitstatus($status);

        return array($wpid, $exit);
    }

    /**
     * @param string $msg
     * @return void
     */
    protected function log($msg)
    {
        if ($this->logger) {
            $this->logger->log($msg);
        }
    }
}
