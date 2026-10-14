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
use AMQPEnvelope;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Asmblah\PhpAmqpCompat\AmqpManager;
use Asmblah\PhpAmqpCompat\Bridge\AmqpBridge;
use Asmblah\PhpAmqpCompat\Connection\Config\DefaultConnectionConfigInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\Channel;
use Envoylope\Amqp\Driver\Sqs\Tests\Functional\AbstractFunctionalTestCase;

/**
 * Class AMQPChannelTest.
 *
 * Tests AMQPChannel against a real ElasticMQ SQS server.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class AMQPChannelTest extends AbstractFunctionalTestCase
{
    private AMQPChannel $amqpChannel;
    private AMQPConnection $amqpConnection;
    private AMQPExchange $amqpExchange;
    private AMQPQueue $amqpQueue;
    private float $uniqueTestIdentifier;

    public function setUp(): void
    {
        parent::setUp();

        $this->resetAmqpManager();
        AmqpManager::setConfiguration($this->createConfiguration());

        $this->uniqueTestIdentifier = microtime(true);
        $this->connect();
    }

    public function tearDown(): void
    {
        parent::tearDown();

        $this->amqpQueue->delete();
        $this->amqpConnection->disconnect();

        $this->resetAmqpManager();
    }

    public function testBasicRecoverRequeuesUnacknowledgedMessages(): void
    {
        $this->amqpExchange->publish('message to recover');

        // Fetch without auto-ack so the message remains unacknowledged.
        $this->amqpQueue->get();

        static::assertTrue($this->amqpChannel->basicRecover(true));

        // Message should be redelivered.
        $envelope = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);
        static::assertSame('message to recover', $envelope->getBody());
    }

    public function testCloseClosesChannel(): void
    {
        $this->amqpChannel->close();

        static::assertFalse($this->amqpChannel->isConnected());

        $this->reconnect();
    }

    public function testGetChannelIdReturnsPositiveInteger(): void
    {
        static::assertGreaterThan(0, $this->amqpChannel->getChannelId());
    }

    public function testGetConnectionReturnsConnection(): void
    {
        static::assertSame($this->amqpConnection, $this->amqpChannel->getConnection());
    }

    public function testGetConsumersIncludesActiveConsumer(): void
    {
        $this->amqpQueue->consume(null, AMQP_NOPARAM, 'test-consumer');

        $consumers = $this->amqpChannel->getConsumers();

        static::assertCount(1, $consumers);
        static::assertArrayHasKey('test-consumer', $consumers);
        static::assertSame($this->amqpQueue, $consumers['test-consumer']);
    }

    public function testGetConsumersIsEmptyInitially(): void
    {
        static::assertSame([], $this->amqpChannel->getConsumers());
    }

    public function testGetGlobalPrefetchCountDefaultsToDefaultGlobalPrefetchCount(): void
    {
        static::assertSame(
            DefaultConnectionConfigInterface::DEFAULT_GLOBAL_PREFETCH_COUNT,
            $this->amqpChannel->getGlobalPrefetchCount()
        );
    }

    public function testGetGlobalPrefetchSizeDefaultsToDefaultGlobalPrefetchSize(): void
    {
        static::assertSame(
            DefaultConnectionConfigInterface::DEFAULT_GLOBAL_PREFETCH_SIZE,
            $this->amqpChannel->getGlobalPrefetchSize()
        );
    }

    public function testGetPrefetchCountDefaultsToDefaultPrefetchCount(): void
    {
        static::assertSame(
            DefaultConnectionConfigInterface::DEFAULT_PREFETCH_COUNT,
            $this->amqpChannel->getPrefetchCount()
        );
    }

    public function testGetPrefetchSizeDefaultsToDefaultPrefetchSize(): void
    {
        static::assertSame(
            DefaultConnectionConfigInterface::DEFAULT_PREFETCH_SIZE,
            $this->amqpChannel->getPrefetchSize()
        );
    }

    public function testIsConnectedReturnsFalseAfterConnectionDisconnects(): void
    {
        $this->amqpConnection->disconnect();

        static::assertFalse($this->amqpChannel->isConnected());

        $this->reconnect();
    }

    public function testIsConnectedReturnsTrueWhenConnected(): void
    {
        static::assertTrue($this->amqpChannel->isConnected());
    }

    public function testQosSetsChannelLocalPrefetchCount(): void
    {
        static::assertTrue($this->amqpChannel->qos(0, 5));

        /** @var Channel $channel */
        $channel = AmqpBridge::getBridgeChannel($this->amqpChannel)
            ->acquireChannel('Bang!');
        static::assertInstanceOf(Channel::class, $channel);
        static::assertSame(5, $channel->getLocalPrefetchCount());
        static::assertSame(0, $channel->getGlobalPrefetchCount());
    }

    public function testQosSetsChannelGlobalPrefetchCount(): void
    {
        static::assertTrue($this->amqpChannel->qos(0, 6, global: true));

        /** @var Channel $channel */
        $channel = AmqpBridge::getBridgeChannel($this->amqpChannel)
            ->acquireChannel('Bang!');
        static::assertInstanceOf(Channel::class, $channel);
        static::assertSame(3, $channel->getLocalPrefetchCount()); // 3 is default for local.
        static::assertSame(6, $channel->getGlobalPrefetchCount());
    }

    public function testSetPrefetchCountChangesPrefetchCountAndClearsPrefetchSize(): void
    {
        $this->amqpChannel->setPrefetchCount(10);

        static::assertSame(10, $this->amqpChannel->getPrefetchCount());
        static::assertSame(0, $this->amqpChannel->getPrefetchSize());
    }

    public function testSetPrefetchCountLimitsMessagesDeliveredToConsumer(): void
    {
        $this->reconnect([
            // Force consumption to exit despite being stalled due to a full prefetch queue.
            'read_timeout' => 2,
        ]);
        $this->amqpChannel->setPrefetchCount(2);
        $receivedMessages = [];

        $this->amqpExchange->publish('message one');
        $this->amqpExchange->publish('message two');
        $this->amqpExchange->publish('message three');
        $this->amqpExchange->publish('message four');
        $this->amqpExchange->publish('message five');

        try {
            $this->amqpQueue->consume(
                function (AMQPEnvelope $envelope, AMQPQueue $queue) use (&$receivedMessages) {
                    $receivedMessages[] = $envelope->getBody();

                    // Ack the first two messages, but leave the third and fourth unacknowledged -
                    // this should fill up the prefetch queue, leaving no room for the fifth to be received,
                    // as it should stay until the timeout is reached to end the consumption.
                    if (count($receivedMessages) < 3) {
                        $queue->ack($envelope->getDeliveryTag());
                    }
                }
            );
        } catch (AMQPException) {
            // Expect the "Consumer timeout exceed" exception when the read timeout is reached.
        }

        static::assertCount(4, $receivedMessages);
        static::assertContains('message one', $receivedMessages);
        static::assertContains('message two', $receivedMessages);
        static::assertContains('message three', $receivedMessages);
        static::assertContains('message four', $receivedMessages);
    }

    public function testSetPrefetchSizeChangesPrefetchSizeAndClearsPrefetchCount(): void
    {
        $this->amqpChannel->setPrefetchCount(5);
        $this->amqpChannel->setPrefetchSize(0);

        static::assertSame(0, $this->amqpChannel->getPrefetchSize());
        static::assertSame(0, $this->amqpChannel->getPrefetchCount());
    }

    /**
     * @param array<string, mixed> $credentials
     */
    private function connect(array $credentials = []): void
    {
        $this->amqpConnection = new AMQPConnection($credentials);
        $this->amqpConnection->connect();
        $this->amqpChannel = new AMQPChannel($this->amqpConnection);

        $this->amqpExchange = new AMQPExchange($this->amqpChannel);
        $this->amqpExchange->setName('exchange-' . $this->uniqueTestIdentifier);
        $this->amqpExchange->setType(AMQP_EX_TYPE_FANOUT);
        $this->amqpExchange->declareExchange();

        $this->amqpQueue = new AMQPQueue($this->amqpChannel);
        $this->amqpQueue->setName('queue-' . $this->uniqueTestIdentifier);
        $this->amqpQueue->setFlags(AMQP_NOPARAM);
        $this->amqpQueue->declareQueue();
        $this->amqpQueue->bind($this->amqpExchange->getName());
    }

    /**
     * @param array<string, mixed> $credentials
     */
    private function reconnect(array $credentials = []): void
    {
        $this->amqpConnection->disconnect();
        $this->connect($credentials);
    }
}
