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

use AMQPEnvelope;
use Asmblah\PhpAmqpCompat\Driver\Common\Channel\ChannelInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Closure;

/**
 * Interface ChannelFactoryInterface.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface ChannelFactoryInterface
{
    /**
     * Creates a channel to SQS.
     */
    public function createChannel(TransportInterface $transport, int $channelId): ChannelInterface;

    /**
     * Creates a consumer for a queue on a channel.
     *
     * @param Closure(AMQPEnvelope): void $callback
     */
    public function createConsumer(
        string $consumerTag,
        string $queueName,
        string $queueUrl,
        Closure $callback,
        bool $autoAck,
        int $prefetchCount
    ): ConsumerInterface;

    /**
     * Creates an envelope, representing the delivery of a message.
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
    ): EnvelopeInterface;
}
