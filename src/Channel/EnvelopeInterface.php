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

/**
 * Interface EnvelopeInterface.
 *
 * Represents a delivery of a message on a channel.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface EnvelopeInterface
{
    /**
     * Acknowledges the message delivery, removing its record from the consumer
     * to free up a slot in the prefetch queue for another message.
     */
    public function acknowledge(): void;

    /**
     * Fetches the body of the message.
     */
    public function getBody(): string;

    /**
     * Fetches the consumer tag for the consumer that consumed this message,
     * or null if it was fetched via AMQPQueue::get() instead of a consumer.
     */
    public function getConsumerTag(): ?string;

    /**
     * Fetches the unique tag of the message delivery for the channel.
     */
    public function getDeliveryTag(): int;

    /**
     * Fetches the decoded AMQP metadata carried on the delivery.
     *
     * An absent or unparseable payload yields an empty array.
     *
     * @return array<string, mixed>
     */
    public function getMetadata(): array;

    /**
     * Fetches the raw, undecoded AMQP metadata JSON carried on the delivery.
     */
    public function getMetadataJson(): string;

    /**
     * Fetches the name of the queue the message was fetched from.
     */
    public function getQueueName(): string;

    /**
     * Fetches the URL of the SQS queue the message was fetched from.
     */
    public function getQueueUrl(): string;

    /**
     * Fetches the unique SQS handle of the message delivery.
     */
    public function getReceiptHandle(): string;

    /**
     * Fetches the original AMQP routing key from the delivery's stored metadata,
     * returning an empty string when the metadata is absent, unparseable, or
     * carries no string routing key.
     */
    public function getRoutingKey(): string;

    /**
     * Determines whether the message was redelivered
     * (i.e. has been previously delivered to a consumer but not acknowledged).
     */
    public function isRedelivered(): bool;
}
