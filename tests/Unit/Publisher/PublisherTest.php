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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Publisher;

use AMQPExchangeException;
use Aws\CommandInterface;
use Aws\Result;
use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Publisher\Publisher;
use Envoylope\Amqp\Driver\Sqs\Registry\DeadLetterExchange;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolverInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class PublisherTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class PublisherTest extends AbstractTestCase
{
    private MockInterface&LoggerInterface $logger;
    private Publisher $publisher;
    private MockInterface&ExchangeRegistryInterface $registry;
    private MockInterface&RouteResolverInterface $routeResolver;
    private MockInterface&SqsClient $sqsClient;

    public function setUp(): void
    {
        $this->logger = mock(LoggerInterface::class);
        $this->registry = mock(ExchangeRegistryInterface::class, [
            // Default: no queue-level TTL or dead-letter exchange configured; individual tests override as needed.
            'getQueueTtlMs' => null,
            'getDeadLetterExchange' => null,
        ]);
        $this->routeResolver = mock(RouteResolverInterface::class);
        $this->sqsClient = mock(SqsClient::class)
            ->shouldAllowMockingMethod('sendMessage');

        $this->publisher = new Publisher($this->sqsClient, $this->registry, $this->routeResolver, $this->logger);
    }

    public function testSendToSqsQueueSendsBodyToTheGivenQueueUrlWithNoDelayWhenNoTtlOrDlxConfigured(): void
    {
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/my_queue', $args['QueueUrl']);
                static::assertSame('hello', $args['MessageBody']);
                static::assertArrayNotHasKey('DelaySeconds', $args);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/my_queue',
            'my_queue',
            'hello',
            [],
            '',
            'my_queue',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueEncodesAmqpAttributesAsAJsonMetadataAttribute(): void
    {
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('String', $args['MessageAttributes']['amqp_metadata']['DataType']);
                /** @var array<string, mixed> $metadata */
                $metadata = json_decode($args['MessageAttributes']['amqp_metadata']['StringValue'], true);
                static::assertSame('my_exchange', $metadata['exchange']);
                static::assertSame('my_key', $metadata['routing_key']);
                static::assertSame('text/plain', $metadata['content_type']);
                static::assertSame(['x-foo' => 'bar'], $metadata['headers']);
                static::assertSame(7, $metadata['priority']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/my_queue',
            'my_queue',
            'hello',
            ['content_type' => 'text/plain', 'priority' => 7, 'headers' => ['x-foo' => 'bar']],
            'my_exchange',
            'my_key',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueDefaultsDeliveryModeAndPriorityWhenAttributesAreAbsent(): void
    {
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                /** @var array<string, mixed> $metadata */
                $metadata = json_decode($args['MessageAttributes']['amqp_metadata']['StringValue'], true);
                static::assertSame(AMQP_DELIVERY_MODE_TRANSIENT, $metadata['delivery_mode']);
                static::assertSame(0, $metadata['priority']);
                static::assertNull($metadata['content_type']);
                static::assertSame([], $metadata['headers']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/my_queue',
            'my_queue',
            'hello',
            [],
            '',
            'my_queue',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueWrapsAndRethrowsWhenTheSqsSendFails(): void
    {
        $this->sqsClient->allows('sendMessage')
            ->andThrow(new SqsException('SQS unavailable', mock(CommandInterface::class)));

        $this->expectException(AMQPExchangeException::class);
        $this->expectExceptionMessage('AMQPExchange::publish(): Failed to publish message: SQS unavailable');

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/my_queue',
            'my_queue',
            'hello',
            [],
            '',
            'my_queue',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueRedirectsThroughTheDeadLetterExchangeWithADelayWhenTheQueueHasTtlAndDlx(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(60000);
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        // No explicit x-dead-letter-routing-key, so the message's own routing key drives the binding match.
        $this->routeResolver->allows('resolveQueueUrls')->with('email.dlx', 'email_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/email', $args['QueueUrl']);
                static::assertSame(60, $args['DelaySeconds']);
                static::assertSame('retry me', $args['MessageBody']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'retry me',
            [],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueUsesTheExplicitDeadLetterRoutingKeyWhenConfigured(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(2000);
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', 'email'));

        $this->routeResolver->expects('resolveQueueUrls')->with('email.dlx', 'email')
            ->once()
            ->andReturn(['email' => 'https://sqs.example.com/email']);
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/email', $args['QueueUrl']);
                static::assertSame(2, $args['DelaySeconds']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'retry me',
            [],
            '',
            'ignored-original-key',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueFansOutTheDelayedRedirectToEveryQueueBoundToTheDeadLetterExchange(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(2000);
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'email_retry')
            ->andReturn([
                'email' => 'https://sqs.example.com/email',
                'audit' => 'https://sqs.example.com/audit',
            ]);

        $seen = [];
        $this->sqsClient->expects('sendMessage')
            ->twice()
            ->andReturnUsing(function (array $args) use (&$seen): Result {
                static::assertSame(2, $args['DelaySeconds']);
                $seen[] = $args['QueueUrl'];

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'retry me',
            [],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );

        sort($seen);
        static::assertSame(['https://sqs.example.com/audit', 'https://sqs.example.com/email'], $seen);
    }

    public function testSendToSqsQueueDropsTheMessageAndWarnsWhenTheDeadLetterExchangeRoutesNowhere(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(2000);
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'email_retry')
            ->andReturn([]);

        $this->sqsClient->expects('sendMessage')
            ->never();
        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertSame(
                    'message TTL elapsed but the dead-letter exchange routed to no queue; message dropped',
                    $message
                );
                static::assertSame('email_retry', $context['queueName']);
                static::assertSame('email.dlx', $context['deadLetterExchange']);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'retry me',
            [],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueDoesNotRedirectWhenTheQueueHasTtlButNoDlx(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(60000);

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertSame(
                    'message and/or queue TTL set but queue has no dead-letter exchange; TTL cannot be emulated and will be ignored',
                    $message
                );
                static::assertSame('email_retry', $context['queueName']);
                static::assertNull($context['messageTtlMs']);
                static::assertSame(60000, $context['queueTtlMs']);
            });
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/email_retry', $args['QueueUrl']);
                static::assertArrayNotHasKey('DelaySeconds', $args);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'no dlx',
            [],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueDoesNotRedirectWhenTheQueueHasDlxButNoTtl(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/email_retry', $args['QueueUrl']);
                static::assertArrayNotHasKey('DelaySeconds', $args);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'no ttl',
            [],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueClampsTheDelayToTheSqsMaximumOf900Seconds(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('slow_retry')
            ->andReturn(1_800_000); // 30 minutes.
        $this->registry->allows()
            ->getDeadLetterExchange('slow_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'slow_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);
        $this->logger->allows('warning');

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(900, $args['DelaySeconds']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/slow_retry',
            'slow_retry',
            'slow',
            [],
            '',
            'slow_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueLogsAWarningWhenTheTtlExceedsTheSqsMaximum(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('slow_retry')
            ->andReturn(1_800_000);
        $this->registry->allows()
            ->getDeadLetterExchange('slow_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'slow_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);
        $this->sqsClient->allows('sendMessage')->andReturn(new Result([]));

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertSame(
                    'message TTL exceeds SQS maximum delivery delay of 900 seconds; clamping to 900s',
                    $message
                );
                static::assertSame('slow_retry', $context['queueName']);
                static::assertSame(1_800_000, $context['ttlMs']);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/slow_retry',
            'slow_retry',
            'slow',
            [],
            '',
            'slow_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueUsesThePerMessageExpirationForTheDelayWhenTheQueueHasDlxAndNoQueueTtl(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'email_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);
        // No queue-level TTL - only the per-message expiration should drive the delay.

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/email', $args['QueueUrl']);
                static::assertSame(5, $args['DelaySeconds']);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'body',
            ['expiration' => '5000'],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueUsesTheMinimumOfExpirationAndQueueTtlWhenTheMessageExpirationIsShorter(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(60_000); // 60s queue TTL.
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'email_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(5, $args['DelaySeconds']); // min(5s, 60s) = 5s.

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'body',
            ['expiration' => '5000'],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueUsesTheMinimumOfExpirationAndQueueTtlWhenTheQueueTtlIsShorter(): void
    {
        $this->registry->allows()
            ->getQueueTtlMs('email_retry')
            ->andReturn(3_000); // 3s queue TTL.
        $this->registry->allows()
            ->getDeadLetterExchange('email_retry')
            ->andReturn(new DeadLetterExchange('email.dlx', null));
        $this->routeResolver->allows()
            ->resolveQueueUrls('email.dlx', 'email_retry')
            ->andReturn(['email' => 'https://sqs.example.com/email']);

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(3, $args['DelaySeconds']); // min(60s, 3s) = 3s.

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/email_retry',
            'email_retry',
            'body',
            ['expiration' => '60000'],
            '',
            'email_retry',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueLogsAWarningWhenTheMessageHasAnExpirationButTheQueueHasNoDlx(): void
    {
        $this->sqsClient->allows('sendMessage')->andReturn(new Result([]));

        $this->logger->expects('warning')
            ->once()
            ->andReturnUsing(function (string $message, array $context): void {
                static::assertSame(
                    'message and/or queue TTL set but queue has no dead-letter exchange; TTL cannot be emulated and will be ignored',
                    $message
                );
                static::assertSame('plain_queue', $context['queueName']);
                static::assertSame(5000, $context['messageTtlMs']);
                static::assertNull($context['queueTtlMs']);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/plain_queue',
            'plain_queue',
            'body',
            ['expiration' => '5000'],
            '',
            'plain_queue',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testSendToSqsQueueStillSendsNormallyWhenTheMessageHasAnExpirationButTheQueueHasNoDlx(): void
    {
        $this->logger->allows('warning');

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/plain_queue', $args['QueueUrl']);
                static::assertArrayNotHasKey('DelaySeconds', $args);

                return new Result([]);
            });

        $this->publisher->sendToSqsQueue(
            'https://sqs.example.com/plain_queue',
            'plain_queue',
            'body',
            ['expiration' => '5000'],
            '',
            'plain_queue',
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }
}
