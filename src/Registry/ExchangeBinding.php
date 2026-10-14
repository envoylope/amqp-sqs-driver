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
 * Class ExchangeBinding.
 *
 * A single exchange-to-exchange binding stored on a source exchange.
 * When a message is published to the source, it is also forwarded to the destination exchange.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ExchangeBinding implements ExchangeBindingInterface
{
    public function __construct(
        private readonly string $destinationExchangeName,
        private readonly string $routingKey
    ) {
    }

    /**
     * @inheritDoc
     */
    public function getDestinationExchangeName(): string
    {
        return $this->destinationExchangeName;
    }

    /**
     * @inheritDoc
     */
    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }
}
