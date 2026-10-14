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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Functional\Topology;

use AMQPChannel;
use AMQPConnection;
use AMQPEnvelope;
use AMQPExchange;
use AMQPQueue;
use Asmblah\PhpAmqpCompat\AmqpManager;
use Asmblah\PhpAmqpCompat\Configuration\Configuration;
use Envoylope\Amqp\Driver\Sqs\Setup\ExistingSetup;
use Envoylope\Amqp\Driver\Sqs\SqsDriver;
use Envoylope\Amqp\Driver\Sqs\Tests\Functional\AbstractFunctionalTestCase;
use Envoylope\Amqp\Driver\Sqs\Topology\CacheBackedTopologyStorage;
use Exception;
use Psr\Log\NullLogger;

/**
 * Class CacheBackedTopologyStorageTest.
 *
 * Functional tests for the CacheBackedTopologyStorage, exercised against a live ElasticMQ server.
 *
 * The warmUp callback is the mechanism for re-declaring topology into the registry after
 * a volatile cache flush (e.g. APCu restart). These tests verify that:
 *   - Publishing and consuming work when topology is declared exclusively by warmUp,
 *     without any explicit AMQPExchange::declareExchange() or AMQPQueue::declareQueue() calls
 *     in the main flow.
 *   - After a cache eviction, warmUp fires again and operations continue to work.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class CacheBackedTopologyStorageTest extends AbstractFunctionalTestCase
{
    private string $exchangeName;
    private string $queueName;
    private string $queueUrl;
    private string $sqsQueueName;

    public function setUp(): void
    {
        parent::setUp();

        $uniqueId = microtime(true);
        $this->exchangeName = 'warmup-ex-' . $uniqueId;
        $this->queueName = 'warmup-q-' . $uniqueId;
        // Mirror the sanitization that Channel::sanitizeQueueName() applies.
        $this->sqsQueueName = preg_replace('/[^a-zA-Z0-9_-]/', '_', $this->queueName);

        // Create the underlying SQS queue up-front so that the warmUp closure can declare
        // its topology. In production, queues are pre-existing infrastructure; warmUp just
        // re-declares the AMQP topology so the driver knows how to route to them.
        $result = $this->sqsClient->createQueue(['QueueName' => $this->sqsQueueName]);
        $this->queueUrl = $result->get('QueueUrl');

        $this->resetAmqpManager();
    }

    public function tearDown(): void
    {
        parent::tearDown();

        try {
            $this->sqsClient->deleteQueue(['QueueUrl' => $this->queueUrl]);
        } catch (Exception) {
        }

        $this->resetAmqpManager();
    }

    public function testPublishAndConsumeWorkWithTopologyDeclaredByWarmUpOnly(): void
    {
        // $channel is assigned before the first AMQP operation that can trigger warmUp.
        /** @var AMQPChannel|null $channel */
        $channel = null;

        // Wire up the driver whose warmUp re-declares the full topology via the AMQP* API.
        // Deliberately NO calls to ->declareExchange() or ->declareQueue() in the main flow -
        // all topology declaration happens exclusively from the warmUp callback.
        AmqpManager::setConfiguration(new Configuration(
            logger: new NullLogger(),
            driver: new SqsDriver(
                new ExistingSetup(
                    $this->sqsClient,
                    new CacheBackedTopologyStorage(
                        cache: $this->cache,
                        warmUp: function () use (&$channel): void {
                            static::assertInstanceOf(AMQPChannel::class, $channel);

                            $exchange = new AMQPExchange($channel);
                            $exchange->setName($this->exchangeName);
                            $exchange->setType(AMQP_EX_TYPE_FANOUT);
                            $exchange->setFlags(AMQP_DURABLE);
                            $exchange->declareExchange();

                            $queue = new AMQPQueue($channel);
                            $queue->setName($this->queueName);
                            $queue->setFlags(AMQP_DURABLE);
                            $queue->declareQueue();
                            $queue->bind($this->exchangeName, '');
                        }
                    )
                )
            )
        ));

        $connection = new AMQPConnection();
        $connection->connect();
        $channel = new AMQPChannel($connection);
        $exchange = new AMQPExchange($channel);
        $exchange->setName($this->exchangeName);
        $exchange->publish('warm-up message');
        $queue = new AMQPQueue($channel);
        $queue->setName($this->queueName);

        $envelope = $queue->get();

        static::assertInstanceOf(AMQPEnvelope::class, $envelope);
        static::assertSame('warm-up message', $envelope->getBody());

        $connection->disconnect();
    }

    public function testWarmUpIsInvokedAgainAfterCacheEvictionAndOperationContinues(): void
    {
        $warmUpCount = 0;

        /** @var AMQPChannel|null $channel */
        $channel = null;

        AmqpManager::setConfiguration(new Configuration(
            logger: new NullLogger(),
            driver: new SqsDriver(
                new ExistingSetup(
                    $this->sqsClient,
                    new CacheBackedTopologyStorage(
                        cache: $this->cache,
                        warmUp: function () use (&$channel, &$warmUpCount): void {
                            static::assertInstanceOf(AMQPChannel::class, $channel);
                            $warmUpCount++;

                            $exchange = new AMQPExchange($channel);
                            $exchange->setName($this->exchangeName);
                            $exchange->setType(AMQP_EX_TYPE_FANOUT);
                            $exchange->setFlags(AMQP_DURABLE);
                            $exchange->declareExchange();

                            $queue = new AMQPQueue($channel);
                            $queue->setName($this->queueName);
                            $queue->setFlags(AMQP_DURABLE);
                            $queue->declareQueue();
                            $queue->bind($this->exchangeName, '');
                        }
                    )
                )
            )
        ));

        $connection = new AMQPConnection();
        $connection->connect();
        $channel = new AMQPChannel($connection);
        $exchange = new AMQPExchange($channel);
        $exchange->setName($this->exchangeName);

        // First publish: topology is absent from cache, so warmUp fires.
        $exchange->publish('message one');
        static::assertSame(1, $warmUpCount);

        // Simulate a volatile cache eviction (e.g. APCu restart, Redis flush).
        $this->cache->clear();

        // Second publish: topology key is gone again, so warmUp fires a second time.
        $exchange->publish('message two');
        static::assertSame(2, $warmUpCount);

        // Both messages should have been routed correctly through the re-warmed topology.
        $queue = new AMQPQueue($channel);
        $queue->setName($this->queueName);

        $first = $queue->get();
        $second = $queue->get();

        static::assertInstanceOf(AMQPEnvelope::class, $first);
        static::assertSame('message one', $first->getBody());
        static::assertInstanceOf(AMQPEnvelope::class, $second);
        static::assertSame('message two', $second->getBody());

        $connection->disconnect();
    }
}
