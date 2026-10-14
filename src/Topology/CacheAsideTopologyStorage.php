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

namespace Envoylope\Amqp\Driver\Sqs\Topology;

use Psr\Cache\CacheItemPoolInterface;

/**
 * Class CacheAsideTopologyStorage.
 *
 * PSR-6 cache-aside for SQS AMQP topology emulation storage.
 *
 * @phpstan-import-type ExchangeData from TopologyStorageInterface
 * @phpstan-import-type QueueData from TopologyStorageInterface
 * @phpstan-import-type Topology from TopologyStorageInterface
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class CacheAsideTopologyStorage implements TopologyStorageInterface
{
    public function __construct(
        private readonly TopologyStorageInterface $backingTopologyStorage,
        private readonly CacheItemPoolInterface $cache
    ) {
    }

    /**
     * @inheritDoc
     */
    public function loadTopology(): array
    {
        $item = $this->cache->getItem(self::TOPOLOGY_KEY);

        if ($item->isHit()) {
            /** @var Topology $loaded */
            $loaded = $item->get();

            return $loaded;
        }

        $topology = $this->backingTopologyStorage->loadTopology();

        $item = $this->cache->getItem(self::TOPOLOGY_KEY);
        $item->set($topology);
        $this->cache->save($item);

        return $topology;
    }

    /**
     * @inheritDoc
     */
    public function updateTopology(callable $callback): array
    {
        $newTopology = $this->backingTopologyStorage->updateTopology($callback);

        $item = $this->cache->getItem(self::TOPOLOGY_KEY);
        $item->set($newTopology);
        $this->cache->save($item);

        return $newTopology;
    }
}
