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

use Envoylope\Amqp\Driver\Sqs\Bridge\MetadataParser;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use JsonException;
use Mockery\MockInterface;

/**
 * Class MetadataParserTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class MetadataParserTest extends AbstractTestCase
{
    private MockInterface&LoggerInterface $logger;
    private MetadataParser $metadataParser;

    public function setUp(): void
    {
        $this->logger = mock(LoggerInterface::class);

        $this->metadataParser = new MetadataParser($this->logger);
    }

    public function testParseReturnsTheDecodedMetadata(): void
    {
        static::assertSame(
            ['exchange' => 'orders', 'routing_key' => 'orders.created'],
            $this->metadataParser->parse('{"exchange":"orders","routing_key":"orders.created"}')
        );
    }

    public function testParseReturnsAnEmptyArrayWhenTheMetadataJsonIsAnEmptyObject(): void
    {
        static::assertSame([], $this->metadataParser->parse('{}'));
    }

    public function testParseLogsAWarningAndReturnsAnEmptyArrayWhenTheMetadataJsonIsMalformed(): void
    {
        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertStringContainsString('Failed to decode AMQP metadata', $message);
                static::assertSame('{not json', $context['invalid_json']);
                static::assertInstanceOf(JsonException::class, $context['exception']);
            });

        static::assertSame([], $this->metadataParser->parse('{not json'));
    }
}
