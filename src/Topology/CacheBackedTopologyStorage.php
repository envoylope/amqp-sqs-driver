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

use Closure;
use Psr\Cache\CacheItemPoolInterface;

/**
 * Class CacheBackedTopologyStorage.
 *
 * PSR-6 cache-backed store for AMQP exchange and queue topology, used to emulate
 * exchange routing on top of AWS SQS.
 * The entire topology is stored under a single cache key so that a volatile
 * cache miss can be detected atomically and recovered via the warmUp callback.
 *
 * @phpstan-import-type ExchangeData from TopologyStorageInterface
 * @phpstan-import-type QueueData from TopologyStorageInterface
 * @phpstan-import-type Topology from TopologyStorageInterface
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class CacheBackedTopologyStorage implements TopologyStorageInterface
{
    /**
     * @param ?Closure $warmUp Called with this registry when topology is absent from the cache.
     *   Callback is expected to declare all exchanges, queues and bindings.
     *   If it declares nothing, an empty topology will be used and no exception raised.
     */
    public function __construct(
        private readonly CacheItemPoolInterface $cache,
        private readonly ?Closure $warmUp = null
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

        // Avoid infinite recursion by ensuring the topology cache is populated first.
        $item->set(['exchanges' => [], 'queues' => []]);
        $this->cache->save($item);

        if ($this->warmUp !== null) {
            ($this->warmUp)();
        }

        // Re-read to pick up whatever warmUp may have declared.
        return $this->loadTopology();
    }

    /**
     * @inheritDoc
     *
     * A PSR-6 cache offers no transactionality, so no special handling of the callback is done.
     */
    public function updateTopology(callable $callback): array
    {
        $topology = $this->loadTopology();

        $newTopology = $callback($topology);

        $item = $this->cache->getItem(self::TOPOLOGY_KEY);
        $item->set($newTopology);
        $this->cache->save($item);

        return $newTopology;
    }
}
