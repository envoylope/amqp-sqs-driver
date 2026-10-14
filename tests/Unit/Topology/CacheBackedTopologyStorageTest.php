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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Topology;

use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Topology\CacheBackedTopologyStorage;
use RuntimeException;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

/**
 * Class CacheBackedTopologyStorageTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class CacheBackedTopologyStorageTest extends AbstractTestCase
{
    private ArrayAdapter $cache;

    public function setUp(): void
    {
        $this->cache = new ArrayAdapter();
    }

    /**
     * Persists the given topology via updateTopology(), discarding the loaded value.
     *
     * @param array<mixed> $topology
     */
    private function saveTopology(CacheBackedTopologyStorage $storage, array $topology): void
    {
        $storage->updateTopology(static fn (): array => $topology);
    }

    public function testWarmUpIsCalledWhenTopologyNotInCache(): void
    {
        $warmUpCalled = false;
        $storage = new CacheBackedTopologyStorage($this->cache, function () use (&$warmUpCalled): void {
            $warmUpCalled = true;
        });

        $storage->loadTopology();

        static::assertTrue($warmUpCalled);
    }

    public function testWarmUpIsNotCalledWhenTopologyIsInCache(): void
    {
        // Populate the cache with a first storage instance so the topology key exists.
        $seed = new CacheBackedTopologyStorage($this->cache);
        $seed->loadTopology();

        $warmUpCalled = false;
        $storage = new CacheBackedTopologyStorage($this->cache, function () use (&$warmUpCalled): void {
            $warmUpCalled = true;
        });
        $storage->loadTopology();

        static::assertFalse($warmUpCalled);
    }

    public function testWarmUpIsCalledOncePerCacheMissNotOnEveryLoad(): void
    {
        $callCount = 0;
        $storage = new CacheBackedTopologyStorage($this->cache, function () use (&$callCount): void {
            $callCount++;
        });

        $storage->loadTopology();
        $storage->loadTopology();
        $storage->loadTopology();

        static::assertSame(1, $callCount);
    }

    public function testWarmUpIsCalledAgainAfterTopologyIsEvictedFromCache(): void
    {
        $callCount = 0;
        $warmUp = function () use (&$callCount): void {
            $callCount++;
        };

        $storage = new CacheBackedTopologyStorage($this->cache, $warmUp);
        $storage->loadTopology();
        static::assertSame(1, $callCount);

        // Simulate a volatile cache eviction (e.g. APCu restart, Redis flush).
        $this->cache->clear();

        // Next load should trigger warmUp again since the topology key is gone.
        $storage->loadTopology();
        static::assertSame(2, $callCount);
    }

    public function testEmptyTopologyIsSavedToCacheAfterWarmUpWithNoDeclarations(): void
    {
        $warmUpCount = 0;
        $warmUp = function () use (&$warmUpCount): void {
            $warmUpCount++;
        };

        // First storage: warmUp runs.
        $storage1 = new CacheBackedTopologyStorage($this->cache, $warmUp);
        $storage1->loadTopology();
        static::assertSame(1, $warmUpCount);

        // Second storage sharing the same cache: topology is now present, warmUp must NOT run again.
        $storage2 = new CacheBackedTopologyStorage($this->cache, $warmUp);
        $storage2->loadTopology();
        static::assertSame(1, $warmUpCount);
    }

    public function testLoadTopologyReturnsEmptyStructureOnCacheMissWithNoWarmUp(): void
    {
        $storage = new CacheBackedTopologyStorage($this->cache);

        $topology = $storage->loadTopology();

        static::assertSame(['exchanges' => [], 'queues' => []], $topology);
    }

    public function testLoadTopologyReturnsPreviouslySavedTopology(): void
    {
        $storage = new CacheBackedTopologyStorage($this->cache);
        $topology = [
            'exchanges' => [
                'my-exchange' => [
                    'type' => AMQP_EX_TYPE_DIRECT,
                    'durable' => true,
                    'autoDelete' => false,
                    'internal' => false,
                    'arguments' => [],
                    'queueBindings' => [],
                    'exchangeBindings' => [],
                ],
            ],
            'queues' => [],
        ];
        $this->saveTopology($storage, $topology);

        $loaded = $storage->loadTopology();

        static::assertSame($topology, $loaded);
    }

    public function testSaveTopologyPersistsAcrossStorageInstances(): void
    {
        $storage1 = new CacheBackedTopologyStorage($this->cache);
        $topology = [
            'exchanges' => [],
            'queues' => [
                'my-queue' => [
                    'url' => 'https://sqs.example.com/my-queue',
                    'durable' => true,
                    'exclusive' => false,
                    'autoDelete' => false,
                    'deadLetterExchange' => null,
                    'deadLetterRoutingKey' => null,
                    'messageTtlMs' => null,
                ],
            ],
        ];

        $this->saveTopology($storage1, $topology);

        $storage2 = new CacheBackedTopologyStorage($this->cache);
        $loaded = $storage2->loadTopology();

        static::assertSame($topology, $loaded);
    }

    public function testSaveTopologyOverwritesPreviousTopology(): void
    {
        $storage = new CacheBackedTopologyStorage($this->cache);
        $this->saveTopology($storage, ['exchanges' => ['old' => [
            'type' => AMQP_EX_TYPE_FANOUT,
            'durable' => false,
            'autoDelete' => false,
            'internal' => false,
            'arguments' => [],
            'queueBindings' => [],
            'exchangeBindings' => [],
        ]], 'queues' => []]);
        $newTopology = ['exchanges' => [], 'queues' => []];

        $this->saveTopology($storage, $newTopology);

        static::assertSame($newTopology, $storage->loadTopology());
    }

    public function testUpdateTopologyPassesLoadedTopologyToCallbackAndPersistsTheReturnValue(): void
    {
        $storage = new CacheBackedTopologyStorage($this->cache);
        $this->saveTopology($storage, ['exchanges' => [], 'queues' => []]);
        $received = null;

        $storage->updateTopology(function (array $topology) use (&$received): array {
            $received = $topology;
            $topology['queues']['q'] = [
                'url' => 'https://sqs.example.com/q',
                'durable' => true,
                'exclusive' => false,
                'autoDelete' => false,
                'deadLetterExchange' => null,
                'deadLetterRoutingKey' => null,
                'messageTtlMs' => null,
            ];

            return $topology;
        });

        static::assertSame(['exchanges' => [], 'queues' => []], $received);
        static::assertArrayHasKey('q', $storage->loadTopology()['queues']);
    }

    public function testUpdateTopologyPropagatesCallbackExceptionLeavingTheStoredTopologyUntouched(): void
    {
        $storage = new CacheBackedTopologyStorage($this->cache);
        $original = ['exchanges' => [], 'queues' => []];
        $this->saveTopology($storage, $original);

        try {
            $storage->updateTopology(static function (array $topology): array {
                throw new RuntimeException('Bang');
            });
            static::fail('Expected exception was not thrown');
        } catch (RuntimeException $exception) {
            static::assertSame('Bang', $exception->getMessage());
        }

        static::assertSame($original, $storage->loadTopology());
    }
}
