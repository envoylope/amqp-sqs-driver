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
 * Class TopicPartMatcher.
 *
 * Recursively matches the dot-separated words of an AMQP topic binding pattern
 * against the words of a message routing key.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class TopicPartMatcher implements TopicPartMatcherInterface
{
    /**
     * @inheritDoc
     */
    public function topicPartMatch(array $binding, array $routing, int $bindingIndex, int $routingIndex): bool
    {
        if ($bindingIndex === count($binding) && $routingIndex === count($routing)) {
            // Both the binding pattern and the routing key have been fully consumed: a match.
            return true;
        }

        if ($bindingIndex === count($binding)) {
            // The binding pattern is exhausted, but routing words remain: no match.
            return false;
        }

        if ($binding[$bindingIndex] === '#') {
            // '#' matches zero or more words: try skipping it or consuming one routing word.
            return $this->topicPartMatch($binding, $routing, $bindingIndex + 1, $routingIndex)
                || ($routingIndex < count($routing)
                    && $this->topicPartMatch($binding, $routing, $bindingIndex, $routingIndex + 1));
        }

        if ($routingIndex === count($routing)) {
            // The routing key is exhausted, but a (non-'#') binding word remains: no match.
            return false;
        }

        if ($binding[$bindingIndex] === '*' || $binding[$bindingIndex] === $routing[$routingIndex]) {
            // '*' matches any single word, or the binding word matches the routing word exactly.
            return $this->topicPartMatch($binding, $routing, $bindingIndex + 1, $routingIndex + 1);
        }

        return false;
    }
}
