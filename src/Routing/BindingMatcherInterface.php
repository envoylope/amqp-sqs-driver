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

namespace Envoylope\Amqp\Driver\Sqs\Routing;

/**
 * Interface BindingMatcherInterface.
 *
 * Determines whether a published message's routing key matches a binding,
 * according to the semantics of the binding's exchange type
 * (fanout / direct / topic).
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface BindingMatcherInterface
{
    /**
     * Determines whether the given AMQP message routing key matches a binding
     * with the given routing key for the given exchange type.
     */
    public function messageMatchesBinding(
        string $exchangeType,
        string $bindingRoutingKey,
        string $messageRoutingKey
    ): bool;
}
