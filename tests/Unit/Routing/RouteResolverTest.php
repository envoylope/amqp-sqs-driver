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

use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeBinding;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\LiveLetterExchange;
use Envoylope\Amqp\Driver\Sqs\Registry\QueueBinding;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolver;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Mockery\MockInterface;

/**
 * Class RouteResolverTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class RouteResolverTest extends AbstractTestCase
{
    private MockInterface&BindingMatcherInterface $bindingMatcher;
    private MockInterface&ExchangeRegistryInterface $registry;
    private RouteResolver $routeResolver;

    public function setUp(): void
    {
        $this->bindingMatcher = mock(BindingMatcherInterface::class);
        $this->registry = mock(ExchangeRegistryInterface::class);

        $this->routeResolver = new RouteResolver($this->registry, $this->bindingMatcher);
    }

    public function testResolveQueueUrlsRoutesToTheQueueNamedByTheRoutingKeyForTheDefaultExchange(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        static::assertSame(
            ['my_queue' => 'https://sqs.example.com/my_queue'],
            $this->routeResolver->resolveQueueUrls('', 'my_queue')
        );
    }

    public function testResolveQueueUrlsReturnsNothingForTheDefaultExchangeWhenTheQueueIsUnknown(): void
    {
        $this->registry->allows()
            ->getQueueUrl('missing')
            ->andReturnNull();

        static::assertSame([], $this->routeResolver->resolveQueueUrls('', 'missing'));
    }

    public function testResolveQueueUrlsReturnsNothingWhenTheExchangeIsNotRegistered(): void
    {
        $this->registry->allows()
            ->getExchange('ghost')
            ->andReturnNull();

        static::assertSame([], $this->routeResolver->resolveQueueUrls('ghost', 'routing_key'));
    }

    public function testResolveQueueUrlsReturnsTheUrlOfADirectlyBoundQueueWhoseBindingMatches(): void
    {
        $this->registry->allows()
            ->getExchange('direct_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_DIRECT,
                'direct_ex',
                [['queue' => 'orders', 'routingKey' => 'new']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'new', 'new')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('orders')
            ->andReturn('https://sqs.example.com/orders');

        static::assertSame(
            ['orders' => 'https://sqs.example.com/orders'],
            $this->routeResolver->resolveQueueUrls('direct_ex', 'new')
        );
    }

    public function testResolveQueueUrlsExcludesADirectlyBoundQueueWhoseBindingDoesNotMatch(): void
    {
        $this->registry->allows()
            ->getExchange('direct_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_DIRECT,
                'direct_ex',
                [['queue' => 'orders', 'routingKey' => 'new']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'new', 'other')
            ->andReturnFalse();

        static::assertSame([], $this->routeResolver->resolveQueueUrls('direct_ex', 'other'));
    }

    public function testResolveQueueUrlsSkipsAMatchingBindingWhoseQueueHasNoRegisteredUrl(): void
    {
        $this->registry->allows()
            ->getExchange('direct_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_DIRECT,
                'direct_ex',
                [['queue' => 'not_registered', 'routingKey' => 'routing_key']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'routing_key', 'routing_key')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('not_registered')
            ->andReturnNull();

        static::assertSame([], $this->routeResolver->resolveQueueUrls('direct_ex', 'routing_key'));
    }

    public function testResolveQueueUrlsFansOutToEveryMatchingQueueForAFanoutExchange(): void
    {
        $this->registry->allows()
            ->getExchange('fanout_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_FANOUT,
                'fanout_ex',
                [
                    ['queue' => 'audit', 'routingKey' => ''],
                    ['queue' => 'search', 'routingKey' => ''],
                ]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_FANOUT, '', 'anything')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('audit')
            ->andReturn('https://sqs.example.com/audit');
        $this->registry->allows()
            ->getQueueUrl('search')
            ->andReturn('https://sqs.example.com/search');

        static::assertSame(
            [
                'audit' => 'https://sqs.example.com/audit',
                'search' => 'https://sqs.example.com/search',
            ],
            $this->routeResolver->resolveQueueUrls('fanout_ex', 'anything')
        );
    }

    public function testResolveQueueUrlsFollowsExchangeToExchangeBindingsRecursively(): void
    {
        $this->registry->allows()
            ->getExchange('source_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_DIRECT,
                'source_ex',
                [],
                [['dest' => 'dest_ex', 'routingKey' => 'routing_key']]
            ));
        $this->registry->allows()
            ->getExchange('dest_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_DIRECT,
                'dest_ex',
                [['queue' => 'final', 'routingKey' => 'routing_key']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'routing_key', 'routing_key')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('final')
            ->andReturn('https://sqs.example.com/final');

        static::assertSame(
            ['final' => 'https://sqs.example.com/final'],
            $this->routeResolver->resolveQueueUrls('source_ex', 'routing_key')
        );
    }

    public function testResolveQueueUrlsDeduplicatesAQueueReachableViaMultiplePaths(): void
    {
        $this->registry->allows()
            ->getExchange('source_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_FANOUT,
                'source_ex',
                [['queue' => 'shared', 'routingKey' => '']],
                [['dest' => 'dest_ex', 'routingKey' => '']]
            ));
        $this->registry->allows()
            ->getExchange('dest_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_FANOUT,
                'dest_ex',
                [['queue' => 'shared', 'routingKey' => '']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_FANOUT, '', 'routing_key')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('shared')
            ->andReturn('https://sqs.example.com/shared');

        static::assertSame(
            ['shared' => 'https://sqs.example.com/shared'],
            $this->routeResolver->resolveQueueUrls('source_ex', 'routing_key')
        );
    }

    public function testResolveQueueUrlsStopsAtAnExchangeBindingCycleRatherThanRecursingForever(): void
    {
        $this->registry->allows()
            ->getExchange('a_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_FANOUT,
                'a_ex',
                [['queue' => 'a_queue', 'routingKey' => '']],
                [['dest' => 'b_ex', 'routingKey' => '']]
            ));
        $this->registry->allows()
            ->getExchange('b_ex')
            ->andReturn($this->createExchange(
                AMQP_EX_TYPE_FANOUT,
                'b_ex',
                [],
                [['dest' => 'a_ex', 'routingKey' => '']]
            ));
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_FANOUT, '', 'routing_key')
            ->andReturnTrue();
        $this->registry->allows()
            ->getQueueUrl('a_queue')
            ->andReturn('https://sqs.example.com/a_queue');

        static::assertSame(
            ['a_queue' => 'https://sqs.example.com/a_queue'],
            $this->routeResolver->resolveQueueUrls('a_ex', 'routing_key')
        );
    }

    /**
     * Builds a LiveLetterExchange as returned by ExchangeRegistry->getExchange().
     *
     * @param list<array{queue: string, routingKey: string}> $queueBindings
     * @param list<array{dest: string, routingKey: string}> $exchangeBindings
     */
    private function createExchange(
        string $type,
        string $name,
        array $queueBindings,
        array $exchangeBindings = []
    ): LiveLetterExchange {
        return new LiveLetterExchange(
            $name,
            $type,
            false,
            false,
            false,
            [],
            array_map(
                static fn (array $binding) => new QueueBinding($binding['queue'], $binding['routingKey']),
                $queueBindings
            ),
            array_map(
                static fn (array $binding) => new ExchangeBinding($binding['dest'], $binding['routingKey']),
                $exchangeBindings
            )
        );
    }
}
