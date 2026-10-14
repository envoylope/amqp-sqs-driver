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
use Envoylope\Amqp\Driver\Sqs\Topology\CacheAsideTopologyStorage;
use Envoylope\Amqp\Driver\Sqs\Topology\TopologyStorageInterface;
use Mockery\MockInterface;
use RuntimeException;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

/**
 * Class CacheAsideTopologyStorageTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class CacheAsideTopologyStorageTest extends AbstractTestCase
{
    private ArrayAdapter $cache;
    private CacheAsideTopologyStorage $storage;
    private MockInterface&TopologyStorageInterface $backingStorage;

    public function setUp(): void
    {
        $this->cache = new ArrayAdapter();
        $this->backingStorage = mock(TopologyStorageInterface::class);

        $this->storage = new CacheAsideTopologyStorage($this->backingStorage, $this->cache);
    }

    /**
     * Builds a topology structure with a single exchange and queue for fixtures.
     *
     * @return array<mixed>
     */
    private function makeTopology(string $exchangeName = 'my-exchange'): array
    {
        return [
            'exchanges' => [
                $exchangeName => [
                    'type' => AMQP_EX_TYPE_DIRECT,
                    'durable' => true,
                    'autoDelete' => false,
                    'internal' => false,
                    'arguments' => [],
                    'queueBindings' => [],
                    'exchangeBindings' => [],
                ],
            ],
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
    }

    public function testLoadTopologyFetchesFromBackingStorageOnCacheMiss(): void
    {
        $topology = $this->makeTopology();

        $this->backingStorage->expects('loadTopology')
            ->once()
            ->andReturn($topology);

        static::assertSame($topology, $this->storage->loadTopology());
    }

    public function testLoadTopologyPopulatesTheCacheOnCacheMiss(): void
    {
        $topology = $this->makeTopology();
        $this->backingStorage->allows('loadTopology')
            ->andReturn($topology);

        $this->storage->loadTopology();

        $item = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY);
        static::assertTrue($item->isHit());
        static::assertSame($topology, $item->get());
    }

    public function testLoadTopologyReturnsCachedValueWithoutHittingBackingStorageOnCacheHit(): void
    {
        $topology = $this->makeTopology();
        $item = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY);
        $item->set($topology);
        $this->cache->save($item);

        $this->backingStorage->expects('loadTopology')
            ->never();

        static::assertSame($topology, $this->storage->loadTopology());
    }

    public function testLoadTopologyOnlyHitsBackingStorageOnceAcrossRepeatedLoads(): void
    {
        $topology = $this->makeTopology();

        $this->backingStorage->expects('loadTopology')
            ->once()
            ->andReturn($topology);

        $this->storage->loadTopology();
        $this->storage->loadTopology();
        $this->storage->loadTopology();
    }

    public function testLoadTopologyCachesAnEmptyTopologySoTheBackingStorageIsNotHitAgain(): void
    {
        $empty = ['exchanges' => [], 'queues' => []];

        $this->backingStorage->expects('loadTopology')
            ->once()
            ->andReturn($empty);

        static::assertSame($empty, $this->storage->loadTopology());
        static::assertSame($empty, $this->storage->loadTopology());
    }

    public function testUpdateTopologyDelegatesToBackingStorageAndReturnsItsResult(): void
    {
        $newTopology = $this->makeTopology('updated-exchange');
        $callback = static fn (array $topology): array => $topology;

        $this->backingStorage->expects()
            ->updateTopology($callback)
            ->once()
            ->andReturn($newTopology);

        static::assertSame($newTopology, $this->storage->updateTopology($callback));
    }

    public function testUpdateTopologyWritesTheNewTopologyToTheCache(): void
    {
        $newTopology = $this->makeTopology('updated-exchange');
        $this->backingStorage->allows('updateTopology')->andReturn($newTopology);

        $this->storage->updateTopology(static fn (array $topology): array => $topology);

        $item = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY);
        static::assertTrue($item->isHit());
        static::assertSame($newTopology, $item->get());
    }

    public function testUpdateTopologyOverwritesAPreviouslyCachedTopology(): void
    {
        $stale = $this->makeTopology('stale-exchange');
        $item = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY);
        $item->set($stale);
        $this->cache->save($item);
        $fresh = $this->makeTopology('fresh-exchange');
        $this->backingStorage->allows('updateTopology')->andReturn($fresh);

        $this->storage->updateTopology(static fn (array $topology): array => $topology);

        static::assertSame($fresh, $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get());
    }

    public function testASubsequentLoadTopologyReturnsTheUpdatedTopologyFromCacheWithoutHittingBackingStorage(): void
    {
        $newTopology = $this->makeTopology('updated-exchange');

        $this->backingStorage->expects('updateTopology')
            ->once()
            ->andReturn($newTopology);
        $this->backingStorage->expects('loadTopology')
            ->never();

        $this->storage->updateTopology(static fn (array $topology): array => $topology);

        static::assertSame($newTopology, $this->storage->loadTopology());
    }

    public function testUpdateTopologyPropagatesABackingStorageExceptionAndLeavesTheCacheUntouched(): void
    {
        $cached = $this->makeTopology('cached-exchange');
        $item = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY);
        $item->set($cached);
        $this->cache->save($item);
        $this->backingStorage->allows('updateTopology')
            ->andThrow(new RuntimeException('Bang'));

        try {
            $this->storage->updateTopology(static fn (array $topology): array => $topology);
            static::fail('Expected exception was not thrown');
        } catch (RuntimeException $exception) {
            static::assertSame('Bang', $exception->getMessage());
        }

        static::assertSame($cached, $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get());
    }
}
