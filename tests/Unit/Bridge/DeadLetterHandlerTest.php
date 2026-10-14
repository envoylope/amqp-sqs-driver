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
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandler;
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\DeadLetterExchange;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolverInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery;
use Mockery\MockInterface;

/**
 * Class DeadLetterHandlerTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class DeadLetterHandlerTest extends AbstractTestCase
{
    private DeadLetterHandler $deadLetterHandler;
    private MockInterface&EnvelopeInterface $envelope;
    private MockInterface&ExchangeRegistryInterface $registry;
    private MockInterface&RouteResolverInterface $routeResolver;
    private MockInterface&SqsClient $sqsClient;

    public function setUp(): void
    {
        $this->registry = mock(ExchangeRegistryInterface::class);
        $this->routeResolver = mock(RouteResolverInterface::class);
        $this->sqsClient = mock(SqsClient::class)
            ->shouldAllowMockingMethod('sendMessage');
        $this->envelope = mock(EnvelopeInterface::class, [
            'getBody' => 'the body',
            'getMetadataJson' => '{"exchange":"orders","routing_key":"orders.created"}',
            'getQueueName' => 'orders_retry',
        ]);

        $this->deadLetterHandler = new DeadLetterHandler(
            $this->sqsClient,
            $this->registry,
            $this->routeResolver
        );
    }

    public function testDeadLetterDoesNothingWhenTheQueueHasNoDeadLetterExchange(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturnNull();

        $this->routeResolver->expects('resolveQueueUrls')
            ->never();
        $this->sqsClient->expects('sendMessage')
            ->never();

        $this->deadLetterHandler->deadLetter($this->envelope);
    }

    public function testDeadLetterUsesTheMessagesOwnRoutingKeyWhenNoDeadLetterRoutingKeyIsConfigured(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturn(new DeadLetterExchange('orders.dlx', null));
        $this->envelope->allows('getRoutingKey')
            ->andReturn('orders.created');

        $this->routeResolver->expects()
            ->resolveQueueUrls('orders.dlx', 'orders.created')
            ->andReturn(['orders' => 'https://sqs.example.com/orders']);
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->with(Mockery::subset(['QueueUrl' => 'https://sqs.example.com/orders']))
            ->andReturn(new Result([]));

        $this->deadLetterHandler->deadLetter($this->envelope);
    }

    public function testDeadLetterUsesTheConfiguredDeadLetterRoutingKeyWithoutConsultingTheEnvelope(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturn(new DeadLetterExchange('orders.dlx', 'requeue'));

        $this->envelope
            ->expects('getRoutingKey')
            ->never();
        $this->routeResolver->expects()
            ->resolveQueueUrls('orders.dlx', 'requeue')
            ->andReturn(['orders' => 'https://sqs.example.com/orders']);
        $this->sqsClient->expects('sendMessage')
            ->once()
            ->with(Mockery::subset(['QueueUrl' => 'https://sqs.example.com/orders']))
            ->andReturn(new Result([]));

        $this->deadLetterHandler->deadLetter($this->envelope);
    }

    public function testDeadLetterSendsTheBodyAndPreservedMetadataToTheResolvedQueue(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturn(new DeadLetterExchange('orders.dlx', 'requeue'));
        $this->routeResolver->allows()
            ->resolveQueueUrls('orders.dlx', 'requeue')
            ->andReturn(['orders' => 'https://sqs.example.com/orders']);

        $this->sqsClient->expects('sendMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame('https://sqs.example.com/orders', $args['QueueUrl']);
                static::assertSame('the body', $args['MessageBody']);
                static::assertSame('String', $args['MessageAttributes']['amqp_metadata']['DataType']);
                static::assertSame(
                    '{"exchange":"orders","routing_key":"orders.created"}',
                    $args['MessageAttributes']['amqp_metadata']['StringValue']
                );

                return new Result([]);
            });

        $this->deadLetterHandler->deadLetter($this->envelope);
    }

    public function testDeadLetterFansOutToEveryQueueBoundToTheDeadLetterExchange(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturn(new DeadLetterExchange('orders.dlx', 'requeue'));
        $this->routeResolver->allows()->resolveQueueUrls('orders.dlx', 'requeue')
            ->andReturn([
                'orders' => 'https://sqs.example.com/orders',
                'audit' => 'https://sqs.example.com/audit',
            ]);

        $sentTo = [];
        $this->sqsClient->expects('sendMessage')
            ->twice()
            ->andReturnUsing(function (array $args) use (&$sentTo): Result {
                $sentTo[] = $args['QueueUrl'];

                return new Result([]);
            });

        $this->deadLetterHandler->deadLetter($this->envelope);

        sort($sentTo);
        static::assertSame(
            [
                'https://sqs.example.com/audit',
                'https://sqs.example.com/orders'
            ],
            $sentTo
        );
    }

    public function testDeadLetterSendsNothingWhenTheDeadLetterExchangeRoutesToNoQueue(): void
    {
        $this->registry->allows()
            ->getDeadLetterExchange('orders_retry')
            ->andReturn(new DeadLetterExchange('orders.dlx', 'requeue'));
        $this->routeResolver->allows()
            ->resolveQueueUrls('orders.dlx', 'requeue')
            ->andReturn([]);

        $this->sqsClient->expects('sendMessage')
            ->never();

        $this->deadLetterHandler->deadLetter($this->envelope);
    }
}
