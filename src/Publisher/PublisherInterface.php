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

namespace Envoylope\Amqp\Driver\Sqs\Publisher;

/**
 * Interface PublisherInterface.
 *
 * Publishes a message to SQS, emulating AMQP message expiry/TTLs/dead-lettering.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface PublisherInterface
{
    /**
     * Sends a message to a specific SQS queue URL, encoding AMQP envelope attributes as a JSON attribute.
     *
     * @param array<string, mixed> $amqpAttributes
     */
    public function sendToSqsQueue(
        string $queueUrl,
        string $queueName,
        string $body,
        array $amqpAttributes,
        string $exchangeName,
        string $routingKey,
        string $exceptionClass,
        string $methodName
    ): void;
}
