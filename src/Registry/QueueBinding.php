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
 * Class QueueBinding.
 *
 * A single exchange-to-queue binding stored on an exchange.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class QueueBinding implements QueueBindingInterface
{
    public function __construct(
        private readonly string $queueName,
        private readonly string $routingKey
    ) {
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
    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }
}
