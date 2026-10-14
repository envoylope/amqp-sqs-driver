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

namespace Envoylope\Amqp\Driver\Sqs\Registry;

/**
 * Class LiveLetterExchange.
 *
 * A declared AMQP exchange, along with its stored topology (type, arguments and bindings),
 * as emulated on top of AWS SQS.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class LiveLetterExchange implements LiveLetterExchangeInterface
{
    /**
     * @param array<string, scalar> $arguments
     * @param list<QueueBindingInterface> $queueBindings
     * @param list<ExchangeBindingInterface> $exchangeBindings
     */
    public function __construct(
        private readonly string $name,
        private readonly string $type,
        private readonly bool $durable,
        private readonly bool $autoDelete,
        private readonly bool $internal,
        private readonly array $arguments,
        private readonly array $queueBindings,
        private readonly array $exchangeBindings
    ) {
    }

    /**
     * @inheritDoc
     */
    public function getArguments(): array
    {
        return $this->arguments;
    }

    /**
     * @inheritDoc
     */
    public function getExchangeBindings(): array
    {
        return $this->exchangeBindings;
    }

    /**
     * @inheritDoc
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @inheritDoc
     */
    public function getQueueBindings(): array
    {
        return $this->queueBindings;
    }

    /**
     * @inheritDoc
     */
    public function getType(): string
    {
        return $this->type;
    }

    /**
     * @inheritDoc
     */
    public function isAutoDelete(): bool
    {
        return $this->autoDelete;
    }

    /**
     * @inheritDoc
     */
    public function isDurable(): bool
    {
        return $this->durable;
    }

    /**
     * @inheritDoc
     */
    public function isInternal(): bool
    {
        return $this->internal;
    }
}
