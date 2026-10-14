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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Routing;

use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcher;
use Envoylope\Amqp\Driver\Sqs\Routing\TopicMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class BindingMatcherTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class BindingMatcherTest extends AbstractTestCase
{
    private BindingMatcher $bindingMatcher;
    private MockInterface&TopicMatcherInterface $topicMatcher;

    public function setUp(): void
    {
        $this->topicMatcher = mock(TopicMatcherInterface::class);

        $this->bindingMatcher = new BindingMatcher($this->topicMatcher);
    }

    public function testMessageMatchesBindingAlwaysReturnsTrueForFanoutExchanges(): void
    {
        $this->topicMatcher->expects('matchTopicPattern')
            ->never();

        static::assertTrue(
            $this->bindingMatcher->messageMatchesBinding(AMQP_EX_TYPE_FANOUT, 'ignored', 'anything')
        );
    }

    public function testMessageMatchesBindingReturnsTrueForDirectExchangeWhenRoutingKeysAreIdentical(): void
    {
        static::assertTrue(
            $this->bindingMatcher->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'my_key', 'my_key')
        );
    }

    public function testMessageMatchesBindingReturnsFalseForDirectExchangeWhenRoutingKeysDiffer(): void
    {
        static::assertFalse(
            $this->bindingMatcher->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'my_key', 'other_key')
        );
    }

    public function testMessageMatchesBindingDelegatesToTopicMatcherForTopicExchangesAndReturnsTrueOnMatch(): void
    {
        $this->topicMatcher->expects('matchTopicPattern')
            ->with('logs.*', 'logs.info')
            ->once()
            ->andReturnTrue();

        static::assertTrue(
            $this->bindingMatcher->messageMatchesBinding(AMQP_EX_TYPE_TOPIC, 'logs.*', 'logs.info')
        );
    }

    public function testMessageMatchesBindingDelegatesToTopicMatcherForTopicExchangesAndReturnsFalseOnNoMatch(): void
    {
        $this->topicMatcher->expects('matchTopicPattern')
            ->with('logs.*', 'logs.info.extra')
            ->once()
            ->andReturnFalse();

        static::assertFalse(
            $this->bindingMatcher->messageMatchesBinding(AMQP_EX_TYPE_TOPIC, 'logs.*', 'logs.info.extra')
        );
    }

    public function testMessageMatchesBindingReturnsFalseForUnknownExchangeTypes(): void
    {
        $this->topicMatcher->expects('matchTopicPattern')
            ->never();

        static::assertFalse(
            $this->bindingMatcher->messageMatchesBinding('headers', 'my_key', 'my_key')
        );
    }
}
