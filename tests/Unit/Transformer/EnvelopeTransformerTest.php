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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Transformer;

use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformer;
use Mockery\MockInterface;

/**
 * Class EnvelopeTransformerTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class EnvelopeTransformerTest extends AbstractTestCase
{
    private EnvelopeTransformer $envelopeTransformer;

    public function setUp(): void
    {
        $this->envelopeTransformer = new EnvelopeTransformer();
    }

    public function testTransformEnvelopeRestoresEveryAmqpPropertyFromTheMetadataAttribute(): void
    {
        $metadata = [
            'exchange' => 'my_exchange',
            'routing_key' => 'my_key',
            'content_type' => 'application/json',
            'content_encoding' => 'gzip',
            'headers' => ['x-foo' => 'bar'],
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT,
            'priority' => 5,
            'correlation_id' => 'corr-1',
            'reply_to' => 'reply.queue',
            'expiration' => '60000',
            'message_id' => 'msg-1',
            'timestamp' => 1_700_000_000,
            'type' => 'my.type',
            'user_id' => 'user-1',
            'app_id' => 'app-1',
            'cluster_id' => 'cluster-1',
        ];

        $envelope = $this->envelopeTransformer->transformEnvelope($this->createSourceEnvelope(
            body: 'the body',
            metadata: $metadata,
            consumerTag: 'consumer-tag',
            deliveryTag: 7,
            redelivered: true
        ));

        static::assertSame('the body', $envelope->getBody());
        static::assertSame('consumer-tag', $envelope->getConsumerTag());
        static::assertSame(7, $envelope->getDeliveryTag());
        static::assertTrue($envelope->isRedelivery());
        static::assertSame('my_exchange', $envelope->getExchangeName());
        static::assertSame('my_key', $envelope->getRoutingKey());
        static::assertSame('application/json', $envelope->getContentType());
        static::assertSame('gzip', $envelope->getContentEncoding());
        static::assertSame(['x-foo' => 'bar'], $envelope->getHeaders());
        static::assertSame(AMQP_DELIVERY_MODE_PERSISTENT, $envelope->getDeliveryMode());
        static::assertSame(5, $envelope->getPriority());
        static::assertSame('corr-1', $envelope->getCorrelationId());
        static::assertSame('reply.queue', $envelope->getReplyTo());
        static::assertSame('60000', $envelope->getExpiration());
        static::assertSame('msg-1', $envelope->getMessageId());
        static::assertSame(1_700_000_000, $envelope->getTimestamp());
        static::assertSame('my.type', $envelope->getType());
        static::assertSame('user-1', $envelope->getUserId());
        static::assertSame('app-1', $envelope->getAppId());
        static::assertSame('cluster-1', $envelope->getClusterId());
    }

    public function testTransformEnvelopeFallsBackToDefaultsWhenTheMetadataIsEmpty(): void
    {
        $envelope = $this->envelopeTransformer->transformEnvelope($this->createSourceEnvelope(
            body: 'bare body',
            metadata: [],
            consumerTag: null,
            deliveryTag: 1,
            redelivered: false
        ));

        static::assertSame('bare body', $envelope->getBody());
        static::assertSame('', $envelope->getExchangeName());
        static::assertSame('', $envelope->getRoutingKey());
        static::assertSame(AMQP_DELIVERY_MODE_TRANSIENT, $envelope->getDeliveryMode());
        static::assertSame(0, $envelope->getPriority());
        static::assertSame([], $envelope->getHeaders());
    }

    /**
     * @param array<string, mixed> $metadata
     */
    private function createSourceEnvelope(
        string $body,
        array $metadata,
        ?string $consumerTag,
        int $deliveryTag,
        bool $redelivered
    ): MockInterface&EnvelopeInterface {
        return mock(EnvelopeInterface::class, [
            'getBody' => $body,
            'getMetadata' => $metadata,
            'getConsumerTag' => $consumerTag,
            'getDeliveryTag' => $deliveryTag,
            'isRedelivered' => $redelivered,
        ]);
    }
}
