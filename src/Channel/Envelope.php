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
 * Class Envelope.
 *
 * Represents a delivery of a message on a channel.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Envelope implements EnvelopeInterface
{
    /**
     * @param array<string, mixed> $metadata Decoded AMQP metadata for the delivery.
     * @param string $metadataJson The raw, undecoded metadata JSON, retained so it can be
     *                             forwarded verbatim (e.g. when dead-lettering) without re-serialising.
     */
    public function __construct(
        private readonly int $deliveryTag,
        private readonly string $receiptHandle,
        private readonly string $queueName,
        private readonly string $queueUrl,
        private readonly string $body,
        private readonly array $metadata,
        private readonly string $metadataJson,
        private readonly bool $redelivered,
        private readonly ?ConsumerInterface $consumer
    ) {
    }

    /**
     * @inheritDoc
     */
    public function acknowledge(): void
    {
        $this->consumer?->acknowledgeEnvelope($this);
    }

    /**
     * @inheritDoc
     */
    public function getBody(): string
    {
        return $this->body;
    }

    /**
     * @inheritDoc
     */
    public function getConsumerTag(): ?string
    {
        return $this->consumer?->getConsumerTag();
    }

    /**
     * @inheritDoc
     */
    public function getDeliveryTag(): int
    {
        return $this->deliveryTag;
    }

    /**
     * @inheritDoc
     */
    public function getMetadata(): array
    {
        return $this->metadata;
    }

    /**
     * @inheritDoc
     */
    public function getMetadataJson(): string
    {
        return $this->metadataJson;
    }

    /**
     * @inheritDoc
     */
    public function getQueueName(): string
    {
        return $this->queueName;
    }

    /**
     * @inheritDoc
     */
    public function getQueueUrl(): string
    {
        return $this->queueUrl;
    }

    /**
     * @inheritDoc
     */
    public function getReceiptHandle(): string
    {
        return $this->receiptHandle;
    }

    /**
     * @inheritDoc
     */
    public function getRoutingKey(): string
    {
        $routingKey = $this->metadata['routing_key'] ?? null;

        return is_string($routingKey) ? $routingKey : '';
    }

    /**
     * @inheritDoc
     */
    public function isRedelivered(): bool
    {
        return $this->redelivered;
    }
}
