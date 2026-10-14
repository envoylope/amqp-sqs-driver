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
 * Class Consumer.
 *
 * Represents a consumer of messages from a queue on a channel.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Consumer implements ConsumerInterface
{
    /**
     * @var array<int, EnvelopeInterface>
     */
    private array $deliveryTagToEnvelopeMap = [];

    /**
     * @param Closure(AMQPEnvelope): void $callback
     */
    public function __construct(
        private readonly string $consumerTag,
        private readonly string $queueName,
        private readonly string $queueUrl,
        private readonly Closure $callback,
        private readonly bool $autoAck,
        private readonly int $prefetchCount
    ) {
    }

    /**
     * @inheritDoc
     */
    public function acknowledgeEnvelope(EnvelopeInterface $envelope): void
    {
        unset($this->deliveryTagToEnvelopeMap[$envelope->getDeliveryTag()]);
    }

    /**
     * @inheritDoc
     */
    public function addUnacknowledgedEnvelope(EnvelopeInterface $envelope): void
    {
        $this->deliveryTagToEnvelopeMap[$envelope->getDeliveryTag()] = $envelope;
    }

    /**
     * @inheritDoc
     */
    public function getAvailablePrefetchCount(): ?int
    {
        return $this->prefetchCount === 0 ?
            null : // Unlimited.
            max(0, $this->prefetchCount - count($this->deliveryTagToEnvelopeMap));
    }

    /**
     * @inheritDoc
     */
    public function getCallback(): Closure
    {
        return $this->callback;
    }

    /**
     * @inheritDoc
     */
    public function getConsumerTag(): string
    {
        return $this->consumerTag;
    }

    /**
     * @inheritDoc
     */
    public function getDeliveryTagToEnvelopeMap(): array
    {
        return $this->deliveryTagToEnvelopeMap;
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
    public function isAutoAck(): bool
    {
        return $this->autoAck;
    }

    /**
     * @inheritDoc
     */
    public function prefetchQueueHasSpace(): bool
    {
        return $this->prefetchCount === 0 || count($this->deliveryTagToEnvelopeMap) < $this->prefetchCount;
    }
}
