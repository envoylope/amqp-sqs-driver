<?php

/*
 * AWS SQS driver for PHP AMQP-Compat.
 * Copyright (c) Dan Phillimore (asmblah)
 * https://github.com/envoylope/amqp-sqs-driver/
 *
 * Released under the MIT license.
 * https://github.com/envoylope/amqp-sqs-driver/raw/main/MIT-LICENSE.txt
 */

declare(strict_types=1);

namespace Envoylope\Amqp\Driver\Sqs;

use Asmblah\PhpAmqpCompat\Configuration\ConfigurationInterface;
use Asmblah\PhpAmqpCompat\Driver\DriverInterface;
use Asmblah\PhpAmqpCompat\Driver\ImplementationInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistry;
use Envoylope\Amqp\Driver\Sqs\Setup\SetupInterface;

/**
 * Class SqsDriver.
 *
 * The entry point for the SQS driver.
 *
 * Usage (no warm-up needed when the topology cache is always populated):
 *   AmqpManager::setConfiguration(new Configuration(
 *       driver: new SqsDriver(
 *           new ExistingSetup(
 *               new SqsClient([...]),
 *               new CacheBackedTopologyStorage(cache: new ArrayAdapter())
 *           )
 *       )
 *   ));
 *
 * Usage with a warm-up callback (required when using a volatile cache):
 *   AmqpManager::setConfiguration(new Configuration(
 *       driver: new SqsDriver(
 *           new ExistingSetup(
 *               new SqsClient([...]),
 *               new CacheBackedTopologyStorage(
 *                   cache: new ApcuAdapter(),
 *                   warmUp: function () use ($channel): void {
 *                       $exchange = new AMQPExchange($channel);
 *                       $exchange->setName('my-exchange');
 *                       $exchange->setType(AMQP_EX_TYPE_DIRECT);
 *                       $exchange->setFlags(AMQP_DURABLE);
 *                       $exchange->declareExchange();
 *
 *                       $queue = new AMQPQueue($channel);
 *                       $queue->setName('my-queue');
 *                       $queue->setFlags(AMQP_DURABLE);
 *                       $queue->declareQueue();
 *                       $queue->bind('my-exchange', 'my-routing-key');
 *                   }
 *               )
 *           )
 *       )
 *   ));
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class SqsDriver implements DriverInterface
{
    public function __construct(
        private readonly SetupInterface $setup
    ) {
    }

    /**
     * @inheritDoc
     */
    public function createImplementation(ConfigurationInterface $configuration): ImplementationInterface
    {
        $sqsClient = $this->setup->getSqsClient();
        $topologyStorage = $this->setup->getTopologyStorage();

        return new SqsImplementation(
            $sqsClient,
            new ExchangeRegistry($topologyStorage),
            $configuration,
            $this->setup->getMaxConsumptionLongPollDurationSeconds(),
            $this->setup->getVisibilityTimeoutSeconds()
        );
    }
}
