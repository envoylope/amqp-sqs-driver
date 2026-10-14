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

use Envoylope\Amqp\Driver\Sqs\Routing\TopicMatcher;
use Envoylope\Amqp\Driver\Sqs\Routing\TopicPartMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class TopicMatcherTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class TopicMatcherTest extends AbstractTestCase
{
    private TopicMatcher $topicMatcher;
    private MockInterface&TopicPartMatcherInterface $topicPartMatcher;

    public function setUp(): void
    {
        $this->topicPartMatcher = mock(TopicPartMatcherInterface::class);

        $this->topicMatcher = new TopicMatcher($this->topicPartMatcher);
    }

    public function testMatchTopicPatternSplitsBothKeysOnDotsAndDelegatesFromTheStartOfEachSequence(): void
    {
        $this->topicPartMatcher->expects('topicPartMatch')
            ->with(['logs', '*', 'errors'], ['logs', 'auth', 'errors'], 0, 0)
            ->once()
            ->andReturnTrue();

        static::assertTrue($this->topicMatcher->matchTopicPattern('logs.*.errors', 'logs.auth.errors'));
    }

    public function testMatchTopicPatternReturnsWhateverThePartMatcherReturns(): void
    {
        $this->topicPartMatcher->expects('topicPartMatch')
            ->with(['logs', 'info'], ['logs', 'warning'], 0, 0)
            ->once()
            ->andReturnFalse();

        static::assertFalse($this->topicMatcher->matchTopicPattern('logs.info', 'logs.warning'));
    }

    public function testMatchTopicPatternTreatsAKeyWithoutDotsAsASingleWord(): void
    {
        $this->topicPartMatcher->expects('topicPartMatch')
            ->with(['logs'], ['logs'], 0, 0)
            ->once()
            ->andReturnTrue();

        static::assertTrue($this->topicMatcher->matchTopicPattern('logs', 'logs'));
    }

    public function testMatchTopicPatternTreatsAnEmptyKeyAsASingleEmptyWord(): void
    {
        $this->topicPartMatcher->expects('topicPartMatch')
            ->with(['#'], [''], 0, 0)
            ->once()
            ->andReturnTrue();

        static::assertTrue($this->topicMatcher->matchTopicPattern('#', ''));
    }
}
