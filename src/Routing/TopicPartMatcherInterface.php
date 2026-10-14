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
 * Interface TopicPartMatcherInterface.
 *
 * Recursively matches the dot-separated words of an AMQP topic binding pattern
 * against the words of a message routing key.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface TopicPartMatcherInterface
{
    /**
     * Determines whether the binding words from index $bindingIndex onwards match
     * the routing words from index $routingIndex onwards.
     *
     * Handles '#' (zero or more words) and '*' (exactly one word) wildcards.
     *
     * @param string[] $binding
     * @param string[] $routing
     */
    public function topicPartMatch(array $binding, array $routing, int $bindingIndex, int $routingIndex): bool;
}
