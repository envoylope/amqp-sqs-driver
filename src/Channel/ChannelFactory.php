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

namespace Envoylope\Amqp\Driver\Sqs\Channel;

use Asmblah\PhpAmqpCompat\Driver\Common\Channel\ChannelInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Asmblah\PhpAmqpCompat\Misc\ClockInterface;
use Aws\Sqs\SqsClient;
use Closure;
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandlerInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\MetadataParserInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspectorInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolverInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\SanitiserInterface;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Publisher\PublisherInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Sleeper\SleeperInterface;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformerInterface;

/**
 * Class ChannelFactory.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ChannelFactory implements ChannelFactoryInterface
{
    public function __construct(
        private readonly SqsClient $sqsClient,
        private readonly ExchangeRegistryInterface $registry,
        private readonly BindingMatcherInterface $bindingMatcher,
        private readonly DeadLetterHandlerInterface $deadLetterHandler,
        private readonly PublisherInterface $publisher,
        private readonly EnvelopeTransformerInterface $envelopeTransformer,
        private readonly MetadataParserInterface $metadataParser,
        private readonly QueueInspectorInterface $queueInspector,
        private readonly QueueUrlResolverInterface $queueUrlResolver,
        private readonly SanitiserInterface $sanitiser,
        private readonly LoggerInterface $logger,
        private readonly ClockInterface $clock,
        private readonly SleeperInterface $sleeper,
        private readonly int $maxConsumptionLongPollDuration,
        private readonly int $messageVisibilityTimeoutSeconds
    ) {
    }

    /**
     * @inheritDoc
     */
    public function createChannel(TransportInterface $transport, int $channelId): ChannelInterface
    {
        return new Channel(
            $this,
            $transport,
            $this->sqsClient,
            $this->registry,
            $this->bindingMatcher,
            $this->deadLetterHandler,
            $this->publisher,
            $this->envelopeTransformer,
            $this->queueInspector,
            $this->queueUrlResolver,
            $this->sanitiser,
            $channelId,
            $this->logger,
            $this->clock,
            $this->sleeper,
            $this->maxConsumptionLongPollDuration,
            $this->messageVisibilityTimeoutSeconds
        );
    }

    /**
     * @inheritDoc
     */
    public function createConsumer(
        string $consumerTag,
        string $queueName,
        string $queueUrl,
        Closure $callback,
        bool $autoAck,
        int $prefetchCount
    ): ConsumerInterface {
        return new Consumer(
            $consumerTag,
            $queueName,
            $queueUrl,
            $callback,
            $autoAck,
            $prefetchCount
        );
    }

    /**
     * @inheritDoc
     */
    public function createEnvelope(
        int $deliveryTag,
        string $receiptHandle,
        string $queueName,
        string $queueUrl,
        string $body,
        string $metadataJson,
        bool $redelivered,
        ?ConsumerInterface $consumer
    ): EnvelopeInterface {
        return new Envelope(
            $deliveryTag,
            $receiptHandle,
            $queueName,
            $queueUrl,
            $body,
            $this->metadataParser->parse($metadataJson),
            $metadataJson,
            $redelivered,
            $consumer
        );
    }
}
