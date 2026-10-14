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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Bridge;

use Envoylope\Amqp\Driver\Sqs\Bridge\Sanitiser;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;

/**
 * Class SanitiserTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class SanitiserTest extends AbstractTestCase
{
    private Sanitiser $sanitiser;

    public function setUp(): void
    {
        $this->sanitiser = new Sanitiser();
    }

    public function testSanitiseQueueNameLeavesAValidNameUntouched(): void
    {
        static::assertSame('my-queue_1', $this->sanitiser->sanitiseQueueName('my-queue_1'));
    }

    public function testSanitiseQueueNameReplacesDisallowedCharactersWithUnderscores(): void
    {
        static::assertSame('my_queue_v2_x', $this->sanitiser->sanitiseQueueName('my.queue/v2 x'));
    }

    public function testSanitiseQueueNameTruncatesOverLongNamesWithAHashSuffix(): void
    {
        $name = str_repeat('a', 100);

        $sanitized = $this->sanitiser->sanitiseQueueName($name);

        static::assertSame(str_repeat('a', 47) . '_' . md5($name), $sanitized);
        static::assertSame(80, strlen($sanitized));
    }
}
