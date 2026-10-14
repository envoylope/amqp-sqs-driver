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
 * Interface TopicMatcherInterface.
 *
 * Matches an AMQP topic routing key pattern against a message routing key.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface TopicMatcherInterface
{
    /**
     * Determines whether the given topic binding pattern matches the given message routing key.
     * Handles '#' (zero or more words) and '*' (exactly one word) wildcards.
     */
    public function matchTopicPattern(string $bindingKey, string $routingKey): bool;
}
