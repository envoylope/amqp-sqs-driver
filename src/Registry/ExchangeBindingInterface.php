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
 * Interface ExchangeBindingInterface.
 *
 * A single exchange-to-exchange binding stored on a source exchange.
 * When a message is published to the source, it is also forwarded to the destination exchange.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface ExchangeBindingInterface
{
    /**
     * Fetches the name of the destination exchange messages are forwarded to.
     */
    public function getDestinationExchangeName(): string;

    /**
     * Fetches the routing key (or pattern) the binding matches against.
     */
    public function getRoutingKey(): string;
}
