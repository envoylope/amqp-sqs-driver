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

use Aws\Result;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspector;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Exception;
use Mockery\MockInterface;

/**
 * Class QueueInspectorTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class QueueInspectorTest extends AbstractTestCase
{
    private MockInterface&LoggerInterface $logger;
    private QueueInspector $queueInspector;
    private MockInterface&SqsClient $sqsClient;

    public function setUp(): void
    {
        $this->logger = mock(LoggerInterface::class);
        $this->sqsClient = mock(SqsClient::class)->shouldAllowMockingMethod('getQueueAttributes');

        $this->queueInspector = new QueueInspector($this->sqsClient, $this->logger);
    }

    public function testFetchApproximateMessageCountReturnsTheAttributeValueAsAnInt(): void
    {
        $this->sqsClient->expects('getQueueAttributes')
            ->with([
                'QueueUrl' => 'https://sqs.example.com/my_queue',
                'AttributeNames' => ['ApproximateNumberOfMessages'],
            ])
            ->once()
            ->andReturn(new Result(['Attributes' => ['ApproximateNumberOfMessages' => '42']]));

        static::assertSame(42, $this->queueInspector->fetchApproximateMessageCount('https://sqs.example.com/my_queue'));
    }

    public function testFetchApproximateMessageCountReturnsZeroWhenTheAttributeIsAbsent(): void
    {
        $this->sqsClient->allows('getQueueAttributes')->andReturn(new Result(['Attributes' => []]));

        static::assertSame(0, $this->queueInspector->fetchApproximateMessageCount('https://sqs.example.com/my_queue'));
    }

    public function testFetchApproximateMessageCountLogsAWarningAndReturnsZeroWhenSqsFails(): void
    {
        $this->sqsClient->allows('getQueueAttributes')->andThrow(new Exception('SQS down'));

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertStringContainsString('Failed to fetch approximate message count', $message);
                static::assertSame('https://sqs.example.com/my_queue', $context['queueUrl']);
            });

        static::assertSame(0, $this->queueInspector->fetchApproximateMessageCount('https://sqs.example.com/my_queue'));
    }
}
