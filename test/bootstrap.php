<?php

spl_autoload_register(function ($class) {
    if (0 === strpos(ltrim($class, '/'), 'Resque\Pool\Test')) {
        if (file_exists($file = __DIR__.'/'.str_replace('\\', '/', $class).'.php')) {
            require_once $file;
        }
    }
});

require_once __DIR__.'/../vendor/autoload.php';
