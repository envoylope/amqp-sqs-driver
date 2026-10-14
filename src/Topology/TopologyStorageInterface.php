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

/**
 * Interface TopologyStorageInterface.
 *
 * Backing store for AMQP exchange and queue topology, used to emulate
 * exchange routing on top of AWS SQS.
 *
 * @phpstan-type ExchangeData array{
 *     type: string,
 *     durable: bool,
 *     autoDelete: bool,
 *     internal: bool,
 *     arguments: array<string, scalar>,
 *     queueBindings: list<array{queue: string, routingKey: string}>,
 *     exchangeBindings: list<array{dest: string, routingKey: string}>
 * }
 * @phpstan-type QueueData array{
 *     url: string,
 *     durable: bool,
 *     exclusive: bool,
 *     autoDelete: bool,
 *     deadLetterExchange: string|null,
 *     deadLetterRoutingKey: string|null,
 *     messageTtlMs: int|null
 * }
 * @phpstan-type Topology array{
 *     exchanges: array<string, ExchangeData>,
 *     queues: array<string, QueueData>
 * }
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface TopologyStorageInterface
{
    public const TOPOLOGY_KEY = 'sqs.topology';

    /**
     * Returns the current topology, always fetching fresh from the backing store.
     *
     * @return Topology
     */
    public function loadTopology(): array;

    /**
     * Atomically reads, mutates, and persists the topology.
     *
     * The callback is passed the currently-stored topology and must return the new
     * topology to persist.
     *
     * Implementations that are able to should wrap the callback invocation in a
     * transaction (e.g. try..catch) so that a thrown exception leaves the previously
     * stored topology intact and the failed mutation is not partially applied.
     *
     * @param callable(Topology): Topology $callback
     * @return Topology
     */
    public function updateTopology(callable $callback): array;
}
