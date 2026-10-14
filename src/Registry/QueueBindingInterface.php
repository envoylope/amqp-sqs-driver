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
 * Interface QueueBindingInterface.
 *
 * A single exchange-to-queue binding stored on an exchange.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface QueueBindingInterface
{
    /**
     * Fetches the name of the bound queue.
     */
    public function getQueueName(): string;

    /**
     * Fetches the routing key (or pattern) the binding matches against.
     */
    public function getRoutingKey(): string;
}
