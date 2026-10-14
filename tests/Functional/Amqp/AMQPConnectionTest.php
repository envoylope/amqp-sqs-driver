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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Functional\Amqp;

use AMQPChannel;
use AMQPConnection;
use AMQPQueue;
use AMQPQueueException;
use Asmblah\PhpAmqpCompat\AmqpManager;
use Envoylope\Amqp\Driver\Sqs\Tests\Functional\AbstractFunctionalTestCase;

/**
 * Class AMQPConnectionTest.
 *
 * Tests AMQPConnection against a live ElasticMQ SQS server.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class AMQPConnectionTest extends AbstractFunctionalTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $this->resetAmqpManager();
        AmqpManager::setConfiguration($this->createConfiguration());
    }

    public function tearDown(): void
    {
        parent::tearDown();

        $this->resetAmqpManager();
    }

    public function testConnectSucceeds(): void
    {
        $amqpConnection = new AMQPConnection();
        $amqpConnection->connect();

        static::assertTrue($amqpConnection->isConnected());

        $amqpConnection->disconnect();
    }

    public function testDisconnectSetsIsConnectedToFalse(): void
    {
        $amqpConnection = new AMQPConnection();
        $amqpConnection->connect();
        $amqpConnection->disconnect();

        static::assertFalse($amqpConnection->isConnected());
    }

    public function testIsConnectedReturnsFalseBeforeConnect(): void
    {
        $amqpConnection = new AMQPConnection();

        static::assertFalse($amqpConnection->isConnected());
    }

    public function testConnectCanReestablishConnection(): void
    {
        $amqpConnection = new AMQPConnection();
        $amqpConnection->connect();
        $amqpConnection->disconnect();
        $amqpConnection->connect();

        static::assertTrue($amqpConnection->isConnected());

        $amqpConnection->disconnect();
    }

    public function testSetReadTimeoutCausesActualTimeoutsDuringReadOperations(): void
    {
        $timeout = 2.5; // 2.5 seconds timeout.
        $amqpConnection = new AMQPConnection();
        $amqpConnection->connect();
        $amqpChannel = new AMQPChannel($amqpConnection);
        $amqpQueue = new AMQPQueue($amqpChannel);
        $amqpQueue->setName('test_timeout_queue_' . uniqid('', true));
        $amqpQueue->setFlags(AMQP_EXCLUSIVE | AMQP_AUTODELETE);
        $amqpQueue->declareQueue();
        $start = microtime(true);

        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $amqpConnection->setReadTimeout($timeout);

        try {
            // Try to consume from the empty queue - this should time out
            // after read-timeout exceeded, as no messages will be received.
            $amqpQueue->consume(function() {
                // This callback should never be called since queue is empty.
                $this->fail('Callback should not be called for empty queue');
            }, AMQP_NOPARAM, 'test_consumer_' . uniqid('', true));
        } catch (AMQPQueueException $exception) {
            $end = microtime(true);
            $actualDuration = $end - $start;

            // Ensure we're reasonably close to the expected timeout.
            static::assertGreaterThan(
                $timeout * 0.8, // At least 80% of expected timeout.
                $actualDuration,
                'Timeout occurred too early'
            );

            static::assertLessThan(
                $timeout * 1.5, // No more than 150% of expected timeout.
                $actualDuration,
                'Timeout took too long'
            );

            throw $exception;
        }
    }
}
