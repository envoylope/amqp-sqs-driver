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

use AMQPQueueException;
use Aws\Command;
use Aws\Exception\AwsException;
use Aws\Result;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolver;
use Envoylope\Amqp\Driver\Sqs\Bridge\SanitiserInterface;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Exception;
use Mockery\MockInterface;

/**
 * Class QueueUrlResolverTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class QueueUrlResolverTest extends AbstractTestCase
{
    private MockInterface&LoggerInterface $logger;
    private QueueUrlResolver $queueUrlResolver;
    private MockInterface&ExchangeRegistryInterface $registry;
    private MockInterface&SanitiserInterface $sanitiser;
    private MockInterface&SqsClient $sqsClient;

    public function setUp(): void
    {
        $this->logger = mock(LoggerInterface::class);
        $this->registry = mock(ExchangeRegistryInterface::class);
        $this->sanitiser = mock(SanitiserInterface::class);
        $this->sqsClient = mock(SqsClient::class)->shouldAllowMockingMethod('getQueueUrl');

        $this->sanitiser->allows('sanitiseQueueName')
            ->andReturnUsing(fn (string $name) => '[sanitised]' . $name . '[/sanitised]')
            ->byDefault();

        $this->queueUrlResolver = new QueueUrlResolver(
            $this->registry,
            $this->sqsClient,
            $this->logger,
            $this->sanitiser
        );
    }

    public function testResolveOrNullReturnsTheRegistryUrlWithoutQueryingSqs(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        $this->sqsClient->expects('getQueueUrl')
            ->never();

        static::assertSame('https://sqs.example.com/my_queue', $this->queueUrlResolver->resolveOrNull('my_queue'));
    }

    public function testResolveOrNullFallsBackToQueryingSqsWithTheSanitisedNameWhenTheRegistryHasNoUrl(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('my.queue')
            ->andReturnNull();

        $this->sqsClient->expects('getQueueUrl')
            ->with(['QueueName' => '[sanitised]my.queue[/sanitised]'])
            ->once()
            ->andReturn(new Result(['QueueUrl' => 'https://sqs.example.com/my_queue']));

        static::assertSame('https://sqs.example.com/my_queue', $this->queueUrlResolver->resolveOrNull('my.queue'));
    }

    public function testResolveOrNullReturnsNullWithoutWarningWhenSqsReportsTheQueueDoesNotExist(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('missing')
            ->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')
            ->andThrow(new AwsException(
                'missing',
                new Command('GetQueueUrl'),
                ['code' => 'QueueDoesNotExist']
            ));

        $this->logger->expects('warning')->never();

        static::assertNull($this->queueUrlResolver->resolveOrNull('missing'));
    }

    public function testResolveOrNullLogsAWarningAndReturnsNullForAnUnexpectedAwsError(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('my_queue')
            ->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')->andThrow(new AwsException(
            'boom',
            new Command('GetQueueUrl'),
            ['code' => 'AccessDenied']
        ));

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertStringContainsString('Unexpected error resolving queue URL', $message);
                static::assertSame('AccessDenied', $context['errorCode']);
            });

        static::assertNull($this->queueUrlResolver->resolveOrNull('my_queue'));
    }

    public function testResolveOrNullLogsAWarningAndReturnsNullForANonAwsFailure(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('my_queue')
            ->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')
            ->andThrow(new Exception('network down'));

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message): void {
                static::assertStringContainsString('Failed to resolve queue URL', $message);
            });

        static::assertNull($this->queueUrlResolver->resolveOrNull('my_queue'));
    }

    public function testResolveReturnsTheUrlWhenTheQueueIsFound(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        static::assertSame(
            'https://sqs.example.com/my_queue',
            $this->queueUrlResolver->resolve('my_queue', AMQPQueueException::class, 'AMQPQueue::get')
        );
    }

    public function testResolveThrowsTheGivenExceptionClassWhenTheQueueIsNotFound(): void
    {
        $this->registry->allows('getQueueUrl')
            ->with('missing')
            ->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')
            ->andThrow(new AwsException(
                'missing',
                new Command('GetQueueUrl'),
                ['code' => 'QueueDoesNotExist']
            ));

        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('AMQPQueue::get(): Queue not found: missing');

        $this->queueUrlResolver->resolve('missing', AMQPQueueException::class, 'AMQPQueue::get');
    }
}
