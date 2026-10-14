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

use Asmblah\PhpAmqpCompat\Driver\Common\Channel\ChannelInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Asmblah\PhpAmqpCompat\Misc\ClockInterface;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandlerInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\MetadataParserInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspectorInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolverInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\SanitiserInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\ChannelFactory;
use Envoylope\Amqp\Driver\Sqs\Channel\ConsumerInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Publisher\PublisherInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Sleeper\SleeperInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformerInterface;
use Mockery\MockInterface;

/**
 * Class ChannelFactoryTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ChannelFactoryTest extends AbstractTestCase
{
    private ChannelFactory $channelFactory;
    private MockInterface&MetadataParserInterface $metadataParser;

    public function setUp(): void
    {
        $this->metadataParser = mock(MetadataParserInterface::class);

        $this->channelFactory = new ChannelFactory(
            mock(SqsClient::class),
            mock(ExchangeRegistryInterface::class),
            mock(BindingMatcherInterface::class),
            mock(DeadLetterHandlerInterface::class),
            mock(PublisherInterface::class),
            mock(EnvelopeTransformerInterface::class),
            $this->metadataParser,
            mock(QueueInspectorInterface::class),
            mock(QueueUrlResolverInterface::class),
            mock(SanitiserInterface::class),
            mock(LoggerInterface::class),
            mock(ClockInterface::class),
            mock(SleeperInterface::class),
            maxConsumptionLongPollDuration: 120,
            messageVisibilityTimeoutSeconds: 100
        );
    }

    public function testCreateChannelReturnsAChannel(): void
    {
        static::assertInstanceOf(
            ChannelInterface::class,
            $this->channelFactory->createChannel(mock(TransportInterface::class), 7)
        );
    }

    public function testCreateConsumerReturnsAConsumerWithTheGivenDetails(): void
    {
        $consumer = $this->channelFactory->createConsumer(
            'consumer-tag',
            'orders',
            'https://sqs.example.com/orders',
            static function (): void {},
            true,
            5
        );

        static::assertInstanceOf(ConsumerInterface::class, $consumer);
        static::assertSame('consumer-tag', $consumer->getConsumerTag());
        static::assertSame('orders', $consumer->getQueueName());
        static::assertTrue($consumer->isAutoAck());
    }

    public function testCreateEnvelopeParsesTheMetadataJsonViaTheParser(): void
    {
        $this->metadataParser->expects('parse')
            ->once()
            ->with('{"routing_key":"orders.created"}')
            ->andReturn(['routing_key' => 'orders.created']);

        $envelope = $this->createEnvelope(metadataJson: '{"routing_key":"orders.created"}');

        static::assertSame(['routing_key' => 'orders.created'], $envelope->getMetadata());
        static::assertSame('orders.created', $envelope->getRoutingKey());
    }

    public function testCreateEnvelopeRetainsTheRawMetadataJson(): void
    {
        $this->metadataParser->allows('parse')
            ->andReturn([]);

        static::assertSame(
            '{"routing_key":"orders.created"}',
            $this->createEnvelope(metadataJson: '{"routing_key":"orders.created"}')->getMetadataJson()
        );
    }

    public function testCreateEnvelopeCarriesThroughTheDeliveryDetails(): void
    {
        $this->metadataParser->allows('parse')
            ->andReturn([]);

        $envelope = $this->createEnvelope(
            deliveryTag: 7,
            receiptHandle: 'receipt-123',
            queueName: 'orders',
            queueUrl: 'https://sqs.example.com/orders',
            body: 'the body',
            redelivered: true
        );

        static::assertSame(7, $envelope->getDeliveryTag());
        static::assertSame('receipt-123', $envelope->getReceiptHandle());
        static::assertSame('orders', $envelope->getQueueName());
        static::assertSame('https://sqs.example.com/orders', $envelope->getQueueUrl());
        static::assertSame('the body', $envelope->getBody());
        static::assertTrue($envelope->isRedelivered());
    }

    private function createEnvelope(
        int $deliveryTag = 1,
        string $receiptHandle = 'receipt',
        string $queueName = 'queue',
        string $queueUrl = 'https://sqs.example.com/queue',
        string $body = 'body',
        string $metadataJson = '{}',
        bool $redelivered = false
    ): EnvelopeInterface {
        return $this->channelFactory->createEnvelope(
            $deliveryTag,
            $receiptHandle,
            $queueName,
            $queueUrl,
            $body,
            $metadataJson,
            $redelivered,
            consumer: null
        );
    }
}
