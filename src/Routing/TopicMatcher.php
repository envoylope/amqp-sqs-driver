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
 * Class TopicMatcher.
 *
 * Matches an AMQP topic routing key pattern against a message routing key.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class TopicMatcher implements TopicMatcherInterface
{
    public function __construct(
        private readonly TopicPartMatcherInterface $topicPartMatcher
    ) {
    }

    /**
     * @inheritDoc
     */
    public function matchTopicPattern(string $bindingKey, string $routingKey): bool
    {
        $bindingParts = explode('.', $bindingKey);
        $routingParts = explode('.', $routingKey);

        return $this->topicPartMatcher->topicPartMatch($bindingParts, $routingParts, 0, 0);
    }
}
