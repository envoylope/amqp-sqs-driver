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
 * Class BindingMatcher.
 *
 * Determines whether a published message's routing key matches a binding,
 * according to the semantics of the binding's exchange type
 * (fanout / direct / topic).
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class BindingMatcher implements BindingMatcherInterface
{
    public function __construct(
        private readonly TopicMatcherInterface $topicMatcher
    ) {
    }

    /**
     * @inheritDoc
     */
    public function messageMatchesBinding(
        string $exchangeType,
        string $bindingRoutingKey,
        string $messageRoutingKey
    ): bool {
        return match ($exchangeType) {
            AMQP_EX_TYPE_FANOUT => true,
            AMQP_EX_TYPE_DIRECT => $messageRoutingKey === $bindingRoutingKey,
            AMQP_EX_TYPE_TOPIC => $this->topicMatcher->matchTopicPattern($bindingRoutingKey, $messageRoutingKey),
            default => false,
        };
    }
}
