PHP Resque Pool
===============

Php resque pool is a port of [resque-pool](http://github.com/nevans/resque-pool)
for managing [php-resque](http://github.com/chrisboulton/php-resque) workers.
Given a config file, it manages your workers for you, starting up the appropriate
number of workers for each worker type.

Benefits
---------

* Less config - With a simple YAML file, you can start up a pool daemon.
* Monitoring - If a worker dies for some reason, php-resque-pool will start
  another.
* Easily change worker distribution - To change your worker counts just update
  the YAML file and send the manager a HUP signal.

How to use
----------

### YAML file config

Create a `config/resque-pool.yml` (or `resque-pool.yml`) with your worker
counts.  The YAML file supports both using root level defaults as well as
environment specific overrides (`RESQUE_ENV` environment variables can be
 used to determine environment).  For example in `config/resque-pool.yml`:

    foo: 1
    bar: 2
    "foo,bar,baz": 1

    production:
      "foo,bar,baz": 4
### Start the pool manager

Then you can start the queues via:

    bin/resque-pool --daemon --environment production

This will start up seven worker processes, one exclusively for the foo queue,
two exclusively for the bar queue, and four workers looking at all queues in
priority.  With the config above, this is similar to if you ran the following:

    QUEUES=foo php resque.php
    QUEUES=bar php resque.php
    QUEUES=bar php resque.php
    QUEUES=foo,bar,baz php resque.php
    QUEUES=foo,bar,baz php resque.php
    QUEUES=foo,bar,baz php resque.php
    QUEUES=foo,bar,baz php resque.php

The pool manager will stay around monitoring the resque worker parents, giving
three levels: a single pool manager, many worker parents, and one worker child
per worker (when the actual job is being processed).  For example, `ps -ef f |
grep [r]esque` (in Linux) might return something like the following:

    resque    13858     1  0 13:44 ?        S      0:02 resque-pool-manager: managing [13867, 13875, 13871, 13872, 13868, 13870, 13876]
    resque    13867 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Waiting for foo
    resque    13868 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Waiting for bar
    resque    13870 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Waiting for bar
    resque    13871 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Waiting for foo,bar,baz
    resque    13872 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Forked 7481 at 1280343254
    resque     7481 13872  0 14:54 ?        S      0:00      \_ resque-1.0: Processing foo since 1280343254
    resque    13875 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Waiting for foo,bar,baz
    resque    13876 13858  0 13:44 ?        S      0:00  \_ resque-1.0: Forked 7485 at 1280343255
    resque     7485 13876  0 14:54 ?        S      0:00      \_ resque-1.0: Processing bar since 1280343254

Running as a daemon will currently output to stdout, although this will be configurable
in the future.

SIGNALS
-------

The pool manager responds to the following signals:

* `HUP`   - reload the config file, restart all workers.
* `QUIT`  - gracefully shut down workers (via `QUIT`) and shutdown the manager
  after all workers are done.
* `INT`   - gracefully shut down workers (via `QUIT`) and immediately shutdown manager
* `TERM`  - immediately shut down workers (via `INT`) and immediately shutdown manager
  _(configurable via command line options)_
* `WINCH` - _(only when running as a daemon)_ send `QUIT` to each worker, but
  keep manager running (send `HUP` to reload config and restart workers)
* `USR1`/`USR2`/`CONT` - pass the signal on to all worker parents (see Resque docs).

Use `HUP` to change the number of workers per worker type.  Signals can be sent via the
 `kill` command, e.g. `kill -HUP $master_pid`

Other Features
--------------

You can specify an alternate config file by setting the `RESQUE_POOL_CONFIG` or
with the `--config` command line option.

Owner
------------

@ebernhardson


Contributors
------------

@michael34435
