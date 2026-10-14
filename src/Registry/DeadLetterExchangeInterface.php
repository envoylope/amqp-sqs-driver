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
 * Interface DeadLetterExchangeInterface.
 *
 * The dead-letter exchange configured for an AMQP queue via `x-dead-letter-exchange`.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface DeadLetterExchangeInterface extends ExchangeInterface
{
    /**
     * Fetches the configured `x-dead-letter-routing-key`, or null to reuse the message's own routing key.
     */
    public function getRoutingKey(): ?string;
}
