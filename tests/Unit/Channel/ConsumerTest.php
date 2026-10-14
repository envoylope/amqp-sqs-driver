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

use AMQPEnvelope;
use Closure;
use Envoylope\Amqp\Driver\Sqs\Channel\Consumer;
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class ConsumerTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ConsumerTest extends AbstractTestCase
{
    private Closure $callback;

    private MockInterface&EnvelopeInterface $envelope;

    public function setUp(): void
    {
        $this->callback = function (AMQPEnvelope $envelope): void {};
        $this->envelope = mock(EnvelopeInterface::class);
        $this->envelope->allows('getDeliveryTag')
            ->andReturn(1);
    }

    public function testAcknowledgeEnvelopeRemovesTheEnvelopeFromTheUnacknowledgedMap(): void
    {
        $consumer = $this->createConsumer();

        $consumer->addUnacknowledgedEnvelope($this->envelope);
        $consumer->acknowledgeEnvelope($this->envelope);

        static::assertSame([], $consumer->getDeliveryTagToEnvelopeMap());
    }

    public function testAcknowledgeEnvelopeIsANoOpWhenTheEnvelopeWasNotUnacknowledged(): void
    {
        $consumer = $this->createConsumer();

        $consumer->acknowledgeEnvelope($this->envelope);

        static::assertSame([], $consumer->getDeliveryTagToEnvelopeMap());
    }

    public function testAcknowledgeEnvelopeDoesNotAffectOtherUnacknowledgedEnvelopes(): void
    {
        $anotherEnvelope = mock(EnvelopeInterface::class);
        $anotherEnvelope->allows('getDeliveryTag')
            ->andReturn(2);
        $consumer = $this->createConsumer();
        $consumer->addUnacknowledgedEnvelope($this->envelope);
        $consumer->addUnacknowledgedEnvelope($anotherEnvelope);

        $consumer->acknowledgeEnvelope($this->envelope);

        static::assertSame([2 => $anotherEnvelope], $consumer->getDeliveryTagToEnvelopeMap());
    }

    public function testAddUnacknowledgedEnvelopeTracksTheEnvelopeByItsDeliveryTag(): void
    {
        $consumer = $this->createConsumer();

        $consumer->addUnacknowledgedEnvelope($this->envelope);

        static::assertSame([1 => $this->envelope], $consumer->getDeliveryTagToEnvelopeMap());
    }

    public function testGetAvailablePrefetchCountReturnsNullWhenThePrefetchCountIsUnlimited(): void
    {
        static::assertNull($this->createConsumer(prefetchCount: 0)->getAvailablePrefetchCount());
    }

    public function testGetAvailablePrefetchCountReturnsTheFullCountWhenNoEnvelopesAreUnacknowledged(): void
    {
        static::assertSame(5, $this->createConsumer(prefetchCount: 5)->getAvailablePrefetchCount());
    }

    public function testGetAvailablePrefetchCountReturnsTheRemainingCountAfterEnvelopesAreUnacknowledged(): void
    {
        $consumer = $this->createConsumer(prefetchCount: 5);
        $consumer->addUnacknowledgedEnvelope($this->envelope);

        static::assertSame(4, $consumer->getAvailablePrefetchCount());
    }

    public function testGetAvailablePrefetchCountNeverReturnsANegativeCount(): void
    {
        $anotherEnvelope = mock(EnvelopeInterface::class);
        $anotherEnvelope->allows('getDeliveryTag')
            ->andReturn(2);
        $consumer = $this->createConsumer(prefetchCount: 1);
        $consumer->addUnacknowledgedEnvelope($this->envelope);
        $consumer->addUnacknowledgedEnvelope($anotherEnvelope);

        static::assertSame(0, $consumer->getAvailablePrefetchCount());
    }

    public function testGetCallbackReturnsTheCallback(): void
    {
        static::assertSame($this->callback, $this->createConsumer()->getCallback());
    }

    public function testGetConsumerTagReturnsTheConsumerTag(): void
    {
        static::assertSame('consumer-tag', $this->createConsumer(consumerTag: 'consumer-tag')->getConsumerTag());
    }

    public function testGetDeliveryTagToEnvelopeMapReturnsAnEmptyMapInitially(): void
    {
        static::assertSame([], $this->createConsumer()->getDeliveryTagToEnvelopeMap());
    }

    public function testGetQueueNameReturnsTheQueueName(): void
    {
        static::assertSame('orders', $this->createConsumer(queueName: 'orders')->getQueueName());
    }

    public function testGetQueueUrlReturnsTheQueueUrl(): void
    {
        static::assertSame(
            'https://sqs.example.com/orders',
            $this->createConsumer(queueUrl: 'https://sqs.example.com/orders')->getQueueUrl()
        );
    }

    public function testIsAutoAckReturnsTrueWhenAutoAckIsEnabled(): void
    {
        static::assertTrue($this->createConsumer(autoAck: true)->isAutoAck());
    }

    public function testIsAutoAckReturnsFalseWhenAutoAckIsDisabled(): void
    {
        static::assertFalse($this->createConsumer(autoAck: false)->isAutoAck());
    }

    public function testPrefetchQueueHasSpaceReturnsTrueWhenThePrefetchCountIsUnlimited(): void
    {
        $consumer = $this->createConsumer(prefetchCount: 0);
        $consumer->addUnacknowledgedEnvelope($this->envelope);

        static::assertTrue($consumer->prefetchQueueHasSpace());
    }

    public function testPrefetchQueueHasSpaceReturnsTrueWhenBelowThePrefetchCount(): void
    {
        static::assertTrue($this->createConsumer(prefetchCount: 1)->prefetchQueueHasSpace());
    }

    public function testPrefetchQueueHasSpaceReturnsFalseWhenExactlyAtThePrefetchCount(): void
    {
        $consumer = $this->createConsumer(prefetchCount: 1);
        $consumer->addUnacknowledgedEnvelope($this->envelope);

        static::assertFalse($consumer->prefetchQueueHasSpace());
    }

    private function createConsumer(
        string $consumerTag = 'consumer-tag',
        string $queueName = 'queue',
        string $queueUrl = 'https://sqs.example.com/queue',
        bool $autoAck = false,
        int $prefetchCount = 0
    ): Consumer {
        return new Consumer(
            $consumerTag,
            $queueName,
            $queueUrl,
            $this->callback,
            $autoAck,
            $prefetchCount
        );
    }
}