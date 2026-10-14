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

use Envoylope\Amqp\Driver\Sqs\Routing\TopicPartMatcher;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Generator;

/**
 * Class TopicPartMatcherTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class TopicPartMatcherTest extends AbstractTestCase
{
    private TopicPartMatcher $topicPartMatcher;

    public function setUp(): void
    {
        $this->topicPartMatcher = new TopicPartMatcher();
    }

    /**
     * @param string[] $binding
     * @param string[] $routing
     * @dataProvider topicPartMatchDataProvider
     */
    public function testTopicPartMatchMatchesFromTheStartOfBothSequences(
        array $binding,
        array $routing,
        bool $expected
    ): void {
        static::assertSame($expected, $this->topicPartMatcher->topicPartMatch($binding, $routing, 0, 0));
    }

    public function testTopicPartMatchHonoursNonZeroStartIndices(): void
    {
        // Starting past the leading 'x' word, 'a.b' matches 'a.b'.
        static::assertTrue($this->topicPartMatcher->topicPartMatch(['x', 'a', 'b'], ['a', 'b'], 1, 0));
    }

    /**
     * @return Generator<string, array{string[], string[], bool}>
     */
    public static function topicPartMatchDataProvider(): Generator
    {
        yield 'single identical word' => [['a'], ['a'], true];
        yield 'multiple identical words' => [['a', 'b', 'c'], ['a', 'b', 'c'], true];
        yield 'differing word' => [['a', 'b'], ['a', 'c'], false];
        yield 'binding longer than routing with no wildcard' => [['a', 'b'], ['a'], false];
        yield 'routing longer than binding with no wildcard' => [['a'], ['a', 'b'], false];

        yield "'*' matches exactly one word" => [['a', '*', 'c'], ['a', 'b', 'c'], true];
        yield "'*' does not match zero words" => [['a', '*', 'c'], ['a', 'c'], false];
        yield "'*' does not match two words" => [['a', '*'], ['a', 'b', 'c'], false];

        yield "'#' matches zero trailing words" => [['a', '#'], ['a'], true];
        yield "'#' matches one trailing word" => [['a', '#'], ['a', 'b'], true];
        yield "'#' matches many trailing words" => [['a', '#'], ['a', 'b', 'c', 'd'], true];
        yield "'#' in the middle matches zero words" => [['a', '#', 'd'], ['a', 'd'], true];
        yield "'#' in the middle matches many words" => [['a', '#', 'd'], ['a', 'b', 'c', 'd'], true];
        yield "'#' in the middle still requires the trailing word" => [['a', '#', 'x'], ['a', 'b', 'c'], false];
        yield "leading '#' matches every word" => [['#'], ['a', 'b', 'c'], true];
        yield "consecutive '#' words match" => [['#', '#'], ['a', 'b'], true];

        yield "'*' then '#' matches" => [['*', '#'], ['a', 'b', 'c'], true];

        yield 'empty binding word matches empty routing word' => [[''], [''], true];
        yield 'empty binding word does not match a real word' => [[''], ['a'], false];
    }
}
