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
use AMQPExchange;
use AMQPQueue;
use Asmblah\PhpAmqpCompat\AmqpManager;
use Envoylope\Amqp\Driver\Sqs\Tests\Functional\AbstractFunctionalTestCase;

/**
 * Class AMQPQueueDeadLetterTest.
 *
 * Tests dead-letter emulation against a live ElasticMQ SQS server.
 *
 * The x-dead-letter-exchange argument names a real, separately-declared exchange (as in RabbitMQ),
 * not a queue. When a message is nacked/rejected without requeue, the driver resolves that exchange
 * through the topology and fans the message out to every bound queue before deleting it from the
 * source queue. No native SQS RedrivePolicy is involved.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class AMQPQueueDeadLetterTest extends AbstractFunctionalTestCase
{
    private AMQPChannel $amqpChannel;
    private AMQPConnection $amqpConnection;
    private AMQPExchange $amqpExchange;
    private AMQPQueue $amqpQueue;
    private AMQPExchange $deadLetterExchange;
    private string $deadLetterExchangeName;
    private string $deadLetterRoutingKey = 'dead-lettered';
    private AMQPQueue $dlqQueue;
    private float $uniqueTestIdentifier;

    public function setUp(): void
    {
        parent::setUp();

        $this->resetAmqpManager();
        AmqpManager::setConfiguration($this->createConfiguration());

        $this->uniqueTestIdentifier = microtime(true);
        $this->deadLetterExchangeName = 'dlx-' . $this->uniqueTestIdentifier;
        $this->connect();
    }

    public function tearDown(): void
    {
        parent::tearDown();

        $this->dlqQueue->delete();
        $this->amqpQueue->delete();
        $this->amqpConnection->disconnect();

        $this->resetAmqpManager();
    }

    public function testNackingMessageWithoutRequeueRoutesToDeadLetterQueue(): void
    {
        $this->amqpExchange->publish('dead-letter-me');

        $envelope = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);

        // Nack without requeue - driver sends message to DLQ, then deletes from main queue.
        static::assertTrue($this->amqpQueue->nack($envelope->getDeliveryTag()));

        // Main queue must now be empty.
        $this->reconnect();
        static::assertFalse($this->amqpQueue->get());

        // DLQ must contain the message.
        $dlqEnvelope = $this->dlqQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $dlqEnvelope);
        static::assertSame('dead-letter-me', $dlqEnvelope->getBody());
        $this->dlqQueue->ack($dlqEnvelope->getDeliveryTag());
    }

    public function testRejectingMessageWithoutRequeueRoutesToDeadLetterQueue(): void
    {
        $this->amqpExchange->publish('reject-me');

        $envelope = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);

        static::assertTrue($this->amqpQueue->reject($envelope->getDeliveryTag()));

        // Main queue must now be empty.
        $this->reconnect();
        static::assertFalse($this->amqpQueue->get());

        // DLQ must contain the message.
        $dlqEnvelope = $this->dlqQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $dlqEnvelope);
        static::assertSame('reject-me', $dlqEnvelope->getBody());
        $this->dlqQueue->ack($dlqEnvelope->getDeliveryTag());
    }

    public function testDeadLetterQueuePreservesMessageBodyAndAmqpMetadata(): void
    {
        $this->amqpExchange->publish(
            'preserved body',
            null,
            AMQP_NOPARAM,
            [
                'content_type' => 'application/json',
                'correlation_id' => 'corr-123',
            ]
        );

        $envelope = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);

        static::assertTrue($this->amqpQueue->nack($envelope->getDeliveryTag()));

        $dlqEnvelope = $this->dlqQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $dlqEnvelope);
        static::assertSame('preserved body', $dlqEnvelope->getBody());
        static::assertSame('application/json', $dlqEnvelope->getContentType());
        static::assertSame('corr-123', $dlqEnvelope->getCorrelationId());
        $this->dlqQueue->ack($dlqEnvelope->getDeliveryTag());
    }

    public function testNackingWithRequeuePutsMessageBackOnMainQueueNotDlq(): void
    {
        $this->amqpExchange->publish('requeue-me');

        $envelope = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);

        // Nack with AMQP_REQUEUE - message must stay on the main queue.
        static::assertTrue($this->amqpQueue->nack($envelope->getDeliveryTag(), AMQP_REQUEUE));

        // DLQ must remain empty.
        static::assertFalse($this->dlqQueue->get());

        // Main queue still has the message.
        $this->reconnect();
        $redelivered = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $redelivered);
        static::assertSame('requeue-me', $redelivered->getBody());
        $this->amqpQueue->ack($redelivered->getDeliveryTag());
    }

    public function testMultipleNacksWithoutRequeueRouteAllToDlq(): void
    {
        $this->amqpExchange->publish('message-one');
        $this->amqpExchange->publish('message-two');

        $envelope1 = $this->amqpQueue->get();
        $envelope2 = $this->amqpQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope1);
        static::assertInstanceOf(AMQPEnvelope::class, $envelope2);

        // Nack both with AMQP_MULTIPLE on the last delivery tag.
        static::assertTrue($this->amqpQueue->nack($envelope2->getDeliveryTag(), AMQP_MULTIPLE));

        $this->reconnect();
        static::assertFalse($this->amqpQueue->get());

        // Both messages must arrive in the DLQ (order not guaranteed by SQS).
        $bodies = [];
        for ($i = 0; $i < 2; $i++) {
            $dlqEnvelope = $this->dlqQueue->get();
            static::assertInstanceOf(AMQPEnvelope::class, $dlqEnvelope);
            $bodies[] = $dlqEnvelope->getBody();
            $this->dlqQueue->ack($dlqEnvelope->getDeliveryTag());
        }
        sort($bodies);
        static::assertEquals(['message-one', 'message-two'], $bodies);
    }

    private function dlqName(): string
    {
        return 'dl-target-' . $this->uniqueTestIdentifier;
    }

    private function connect(): void
    {
        $this->amqpConnection = new AMQPConnection();
        $this->amqpConnection->connect();
        $this->amqpChannel = new AMQPChannel($this->amqpConnection);

        $this->amqpExchange = new AMQPExchange($this->amqpChannel);
        $this->amqpExchange->setName('exchange-dl-' . $this->uniqueTestIdentifier);
        $this->amqpExchange->setType(AMQP_EX_TYPE_FANOUT);
        $this->amqpExchange->declareExchange();

        // The dead-letter exchange: a real, separately-declared direct exchange.
        $this->deadLetterExchange = new AMQPExchange($this->amqpChannel);
        $this->deadLetterExchange->setName($this->deadLetterExchangeName);
        $this->deadLetterExchange->setType(AMQP_EX_TYPE_DIRECT);
        $this->deadLetterExchange->declareExchange();

        $this->amqpQueue = new AMQPQueue($this->amqpChannel);
        $this->amqpQueue->setName('main-' . $this->uniqueTestIdentifier);
        $this->amqpQueue->setFlags(AMQP_NOPARAM);
        $this->amqpQueue->setArgument('x-dead-letter-exchange', $this->deadLetterExchangeName);
        $this->amqpQueue->setArgument('x-dead-letter-routing-key', $this->deadLetterRoutingKey);
        $this->amqpQueue->declareQueue();
        $this->amqpQueue->bind($this->amqpExchange->getName());

        // The dead-letter target queue, bound to the dead-letter exchange under the dead-letter routing key.
        $this->dlqQueue = new AMQPQueue($this->amqpChannel);
        $this->dlqQueue->setName($this->dlqName());
        $this->dlqQueue->setFlags(AMQP_NOPARAM);
        $this->dlqQueue->declareQueue();
        $this->dlqQueue->bind($this->deadLetterExchangeName, $this->deadLetterRoutingKey);
    }

    private function reconnect(): void
    {
        $this->amqpConnection->disconnect();
        $this->connect();
    }
}
