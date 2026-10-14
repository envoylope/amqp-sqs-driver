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

namespace Envoylope\Amqp\Driver\Sqs\Bridge;

/**
 * Interface QueueUrlResolverInterface.
 *
 * Maps AMQP queue names to their backing SQS queue URLs, consulting the topology registry first
 * and falling back to querying SQS directly, and sanitises AMQP queue names into valid SQS names.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface QueueUrlResolverInterface
{
    /**
     * Resolves the SQS queue URL for the given AMQP queue name, throwing an instance of
     * $exceptionClass (message prefixed with $methodName) when it cannot be found.
     */
    public function resolve(string $queueName, string $exceptionClass, string $methodName): string;

    /**
     * Resolves the SQS queue URL for the given AMQP queue name, returning null when it cannot be found.
     */
    public function resolveOrNull(string $queueName): ?string;
}
