# AWS SQS driver for PHP AMQP-Compat

[![Build Status](https://github.com/envoylope/amqp-sqs-driver/workflows/CI/badge.svg)](https://github.com/envoylope/amqp-sqs-driver/actions?query=workflow%3ACI)

AWS SQS driver for [PHP AMQP-Compat][].

Emulates AMQP concepts such as exchange routing on top of SQS.

## Usage

Install via Composer:

```shell
$ composer require envoylope/amqp-sqs-driver
```

Then configure as the driver for PHP AMQP-Compat, e.g. if using Nytris platform:

`nytris.config.php`
```
...
use Envoylope\Amqp\Driver\Sqs\SqsDriver;
...
$bootConfig->installPackage(new AmqpCompatPackage(
    ...
    driver: new SqsDriver(setup: ...)
));
...
```

Setup examples:

No warm-up needed when the topology cache is always populated:
```
$bootConfig->installPackage(new AmqpCompatPackage(
    driver: new SqsDriver(
        new ExistingSetup(
            new SqsClient([...]),
            new CacheBackedTopologyStorage(cache: new ArrayAdapter())
        )
    )
));
```

Usage with a warm-up callback (required when using a volatile cache):
```
$bootConfig->installPackage(new AmqpCompatPackage(
    driver: new SqsDriver(
        new ExistingSetup(
            new SqsClient([...]),
            new CacheBackedTopologyStorage(
                cache: new ApcuAdapter(),
                warmUp: function () use ($channel): void {
                    $exchange = new AMQPExchange($channel);
                    $exchange->setName('my-exchange');
                    $exchange->setType(AMQP_EX_TYPE_DIRECT);
                    $exchange->setFlags(AMQP_DURABLE);
                    $exchange->declareExchange();

                    $queue = new AMQPQueue($channel);
                    $queue->setName('my-queue');
                    $queue->setFlags(AMQP_DURABLE);
                    $queue->declareQueue();
                    $queue->bind('my-exchange', 'my-routing-key');
                }
            )
        )
    )
));
```

[PHP AMQP-Compat]: https://github.com/asmblah/php-amqp-compat
