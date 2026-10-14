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
use Closure;

/**
 * Interface ConsumerInterface.
 *
 * Represents a consumer of messages from a queue on a channel.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface ConsumerInterface
{
    /**
     * Marks the envelope as (negatively-)acknowledged.
     */
    public function acknowledgeEnvelope(EnvelopeInterface $envelope): void;

    /**
     * Marks an envelope as received by this consumer but not yet (negatively-)acknowledged.
     */
    public function addUnacknowledgedEnvelope(EnvelopeInterface $envelope): void;

    /**
     * Fetches the number of unacknowledged messages the consumer can still receive, or null for unlimited.
     */
    public function getAvailablePrefetchCount(): ?int;

    /**
     * Fetches the callback to call with the received message in order to consume it.
     *
     * @return Closure(AMQPEnvelope): void
     */
    public function getCallback(): Closure;

    /**
     * Fetches the unique tag of this consumer on the channel.
     */
    public function getConsumerTag(): string;

    /**
     * Fetches the map of delivery tag to unacknowledged envelope.
     *
     * @return array<int, EnvelopeInterface>
     */
    public function getDeliveryTagToEnvelopeMap(): array;

    /**
     * Fetches the name of the queue the messages will be consumed from.
     */
    public function getQueueName(): string;

    /**
     * Fetches the SQS URL of the queue.
     */
    public function getQueueUrl(): string;

    /**
     * Determines whether messages are auto-acknowledged for this consumer.
     */
    public function isAutoAck(): bool;

    /**
     * Determines whether this consumer's prefetch queue has space for more messages.
     */
    public function prefetchQueueHasSpace(): bool;
}
