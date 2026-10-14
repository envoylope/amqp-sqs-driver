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
 * Class DeadLetterExchange.
 *
 * The dead-letter exchange configured for an AMQP queue via `x-dead-letter-exchange`.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class DeadLetterExchange implements DeadLetterExchangeInterface
{
    public function __construct(
        private readonly string $name,
        private readonly ?string $routingKey
    ) {
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
    public function getRoutingKey(): ?string
    {
        return $this->routingKey;
    }
}
