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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Channel;

use Envoylope\Amqp\Driver\Sqs\Channel\ConsumerInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\Envelope;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class EnvelopeTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class EnvelopeTest extends AbstractTestCase
{
    private MockInterface&ConsumerInterface $consumer;

    public function setUp(): void
    {
        $this->consumer = mock(ConsumerInterface::class);
    }

    public function testAcknowledgeDelegatesToTheConsumer(): void
    {
        $envelope = $this->createEnvelope();

        $this->consumer->expects('acknowledgeEnvelope')
            ->once()
            ->with($envelope);

        $envelope->acknowledge();
    }

    public function testAcknowledgeIsANoOpWhenThereIsNoConsumer(): void
    {
        $this->expectNotToPerformAssertions();

        $this->createEnvelope(consumer: null)->acknowledge();
    }

    public function testGetBodyReturnsTheBody(): void
    {
        static::assertSame('the body', $this->createEnvelope(body: 'the body')->getBody());
    }

    public function testGetConsumerTagReturnsTheConsumerTag(): void
    {
        $this->consumer->allows('getConsumerTag')
            ->andReturn('consumer-tag');

        static::assertSame('consumer-tag', $this->createEnvelope()->getConsumerTag());
    }

    public function testGetConsumerTagReturnsNullWhenThereIsNoConsumer(): void
    {
        static::assertNull($this->createEnvelope(consumer: null)->getConsumerTag());
    }

    public function testGetDeliveryTagReturnsTheDeliveryTag(): void
    {
        static::assertSame(42, $this->createEnvelope(deliveryTag: 42)->getDeliveryTag());
    }

    public function testGetMetadataReturnsTheMetadata(): void
    {
        $metadata = ['exchange' => 'orders', 'routing_key' => 'orders.created'];

        static::assertSame($metadata, $this->createEnvelope(metadata: $metadata)->getMetadata());
    }

    public function testGetMetadataJsonReturnsTheRawUndecodedJson(): void
    {
        static::assertSame(
            '{not json',
            $this->createEnvelope(metadataJson: '{not json')->getMetadataJson()
        );
    }

    public function testGetQueueNameReturnsTheQueueName(): void
    {
        static::assertSame('orders', $this->createEnvelope(queueName: 'orders')->getQueueName());
    }

    public function testGetQueueUrlReturnsTheQueueUrl(): void
    {
        static::assertSame(
            'https://sqs.example.com/orders',
            $this->createEnvelope(queueUrl: 'https://sqs.example.com/orders')->getQueueUrl()
        );
    }

    public function testGetReceiptHandleReturnsTheReceiptHandle(): void
    {
        static::assertSame('receipt-123', $this->createEnvelope(receiptHandle: 'receipt-123')->getReceiptHandle());
    }

    public function testGetRoutingKeyReturnsTheRoutingKeyFromTheMetadata(): void
    {
        static::assertSame(
            'orders.created',
            $this->createEnvelope(metadata: ['routing_key' => 'orders.created'])->getRoutingKey()
        );
    }

    public function testGetRoutingKeyReturnsAnEmptyStringWhenTheMetadataHasNoRoutingKey(): void
    {
        static::assertSame('', $this->createEnvelope(metadata: ['exchange' => 'my_ex'])->getRoutingKey());
    }

    public function testGetRoutingKeyReturnsAnEmptyStringWhenTheMetadataIsEmpty(): void
    {
        static::assertSame('', $this->createEnvelope(metadata: [])->getRoutingKey());
    }

    public function testGetRoutingKeyReturnsAnEmptyStringWhenTheRoutingKeyIsNotAString(): void
    {
        static::assertSame('', $this->createEnvelope(metadata: ['routing_key' => 42])->getRoutingKey());
    }

    public function testIsRedeliveredReturnsTrueWhenTheMessageWasRedelivered(): void
    {
        static::assertTrue($this->createEnvelope(redelivered: true)->isRedelivered());
    }

    public function testIsRedeliveredReturnsFalseWhenTheMessageWasNotRedelivered(): void
    {
        static::assertFalse($this->createEnvelope(redelivered: false)->isRedelivered());
    }

    /**
     * @param array<string, mixed> $metadata
     */
    private function createEnvelope(
        int $deliveryTag = 1,
        string $receiptHandle = 'receipt',
        string $queueName = 'queue',
        string $queueUrl = 'https://sqs.example.com/queue',
        string $body = 'body',
        array $metadata = [],
        string $metadataJson = '{}',
        bool $redelivered = false,
        ConsumerInterface|null|false $consumer = false
    ): Envelope {
        return new Envelope(
            $deliveryTag,
            $receiptHandle,
            $queueName,
            $queueUrl,
            $body,
            $metadata,
            $metadataJson,
            $redelivered,
            $consumer === false ? $this->consumer : $consumer
        );
    }
}
