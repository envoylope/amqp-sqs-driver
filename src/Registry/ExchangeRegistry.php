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

namespace Envoylope\Amqp\Driver\Sqs\Registry;

use Envoylope\Amqp\Driver\Sqs\Topology\TopologyStorageInterface;

/**
 * Class ExchangeRegistry.
 *
 * Store for AMQP exchange and queue topology.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ExchangeRegistry implements ExchangeRegistryInterface
{
    public function __construct(
        private readonly TopologyStorageInterface $topologyStorage
    ) {
    }

    /**
     * @inheritDoc
     */
    public function addExchangeBinding(string $sourceExchangeName, string $destExchangeName, string $routingKey): void
    {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use ($sourceExchangeName, $destExchangeName, $routingKey): array {
                if (!isset($topology[self::EXCHANGES][$sourceExchangeName])) {
                    // TODO: Assuming this must mean the source exchange hasn't been declared, throw an exception?
                    return $topology;
                }

                foreach ($topology[self::EXCHANGES][$sourceExchangeName][self::EXCHANGE_BINDINGS] as $existing) {
                    if ($existing[self::DESTINATION_EXCHANGE] === $destExchangeName && $existing[self::ROUTING_KEY] === $routingKey) {
                        // Binding already exists; this is an idempotent operation, nothing to do.
                        return $topology;
                    }
                }

                $topology[self::EXCHANGES][$sourceExchangeName][self::EXCHANGE_BINDINGS][] = [
                    self::DESTINATION_EXCHANGE => $destExchangeName,
                    self::ROUTING_KEY => $routingKey,
                ];

                return $topology;
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function addQueueBinding(string $exchangeName, string $queueName, string $routingKey): void
    {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use ($exchangeName, $queueName, $routingKey): array {
                if (!isset($topology[self::EXCHANGES][$exchangeName])) {
                    // TODO: As above.
                    return $topology;
                }

                foreach ($topology[self::EXCHANGES][$exchangeName][self::QUEUE_BINDINGS] as $existing) {
                    if ($existing[self::QUEUE] === $queueName && $existing[self::ROUTING_KEY] === $routingKey) {
                        // Binding already exists; this is an idempotent operation, nothing to do.
                        return $topology;
                    }
                }

                $topology[self::EXCHANGES][$exchangeName][self::QUEUE_BINDINGS][] = [
                    self::QUEUE => $queueName,
                    self::ROUTING_KEY => $routingKey,
                ];

                return $topology;
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function deleteExchange(string $name): void
    {
        $this->topologyStorage->updateTopology(static function (array $topology) use ($name): array {
            unset($topology[self::EXCHANGES][$name]);

            return $topology;
        });
    }

    /**
     * @inheritDoc
     */
    public function deleteQueue(string $name): void
    {
        $this->topologyStorage->updateTopology(static function (array $topology) use ($name): array {
            unset($topology[self::QUEUES][$name]);

            return $topology;
        });
    }

    /**
     * @inheritDoc
     */
    public function getDeadLetterExchange(string $queueName): ?DeadLetterExchangeInterface
    {
        $queue = $this->topologyStorage->loadTopology()[self::QUEUES][$queueName] ?? null;

        if ($queue === null || $queue[self::DEAD_LETTER_EXCHANGE] === null) {
            return null;
        }

        return new DeadLetterExchange($queue[self::DEAD_LETTER_EXCHANGE], $queue[self::DEAD_LETTER_ROUTING_KEY]);
    }

    /**
     * @inheritDoc
     */
    public function getExchange(string $name): ?LiveLetterExchangeInterface
    {
        $exchange = $this->topologyStorage->loadTopology()[self::EXCHANGES][$name] ?? null;

        if ($exchange === null) {
            return null;
        }

        return new LiveLetterExchange(
            $name,
            $exchange[self::EXCHANGE_TYPE],
            $exchange[self::DURABLE],
            $exchange[self::AUTO_DELETE],
            $exchange[self::INTERNAL],
            $exchange[self::ARGUMENTS],
            array_map(
                static fn (array $binding): QueueBindingInterface => new QueueBinding(
                    $binding[self::QUEUE],
                    $binding[self::ROUTING_KEY]
                ),
                $exchange[self::QUEUE_BINDINGS]
            ),
            array_map(
                static fn (array $binding): ExchangeBindingInterface => new ExchangeBinding(
                    $binding[self::DESTINATION_EXCHANGE],
                    $binding[self::ROUTING_KEY]
                ),
                $exchange[self::EXCHANGE_BINDINGS]
            )
        );
    }

    /**
     * @inheritDoc
     */
    public function getQueueTtlMs(string $queueName): ?int
    {
        return $this->topologyStorage->loadTopology()[self::QUEUES][$queueName][self::MESSAGE_TTL_MS] ?? null;
    }

    /**
     * @inheritDoc
     */
    public function getQueueUrl(string $queueName): ?string
    {
        return $this->topologyStorage->loadTopology()[self::QUEUES][$queueName][self::URL] ?? null;
    }

    /**
     * @inheritDoc
     */
    public function removeExchangeBinding(
        string $sourceExchangeName,
        string $destExchangeName,
        string $routingKey
    ): void {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use ($sourceExchangeName, $destExchangeName, $routingKey): array {
                if (!isset($topology[self::EXCHANGES][$sourceExchangeName])) {
                    return $topology;
                }

                $topology[self::EXCHANGES][$sourceExchangeName][self::EXCHANGE_BINDINGS] = array_values(array_filter(
                    $topology[self::EXCHANGES][$sourceExchangeName][self::EXCHANGE_BINDINGS],
                    static fn (array $binding) => !($binding[self::DESTINATION_EXCHANGE] === $destExchangeName && $binding[self::ROUTING_KEY] === $routingKey)
                ));

                return $topology;
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function removeQueueBinding(string $exchangeName, string $queueName, string $routingKey): void
    {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use ($exchangeName, $queueName, $routingKey): array {
                if (!isset($topology[self::EXCHANGES][$exchangeName])) {
                    return $topology;
                }

                $topology[self::EXCHANGES][$exchangeName][self::QUEUE_BINDINGS] = array_values(array_filter(
                    $topology[self::EXCHANGES][$exchangeName][self::QUEUE_BINDINGS],
                    static fn (array $binding) => !($binding[self::QUEUE] === $queueName && $binding[self::ROUTING_KEY] === $routingKey)
                ));

                return $topology;
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function setExchange(
        string $name,
        string $type,
        bool $durable,
        bool $autoDelete,
        bool $internal,
        array $arguments
    ): void {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use ($name, $type, $durable, $autoDelete, $internal, $arguments): array {
                $existing = $topology[self::EXCHANGES][$name] ?? null;

                $topology[self::EXCHANGES][$name] = [
                    self::EXCHANGE_TYPE => $type,
                    self::DURABLE => $durable,
                    self::AUTO_DELETE => $autoDelete,
                    self::INTERNAL => $internal,
                    self::ARGUMENTS => $arguments,
                    // Preserve existing bindings on re-declaration.
                    self::QUEUE_BINDINGS => $existing[self::QUEUE_BINDINGS] ?? [],
                    self::EXCHANGE_BINDINGS => $existing[self::EXCHANGE_BINDINGS] ?? [],
                ];

                return $topology;
            }
        );
    }

    /**
     * @inheritDoc
     */
    public function setQueue(
        string $name,
        string $url,
        bool $durable,
        bool $exclusive,
        bool $autoDelete,
        ?string $deadLetterExchange = null,
        ?string $deadLetterRoutingKey = null,
        ?int $messageTtlMs = null
    ): void {
        $this->topologyStorage->updateTopology(
            static function (array $topology) use (
                $name,
                $url,
                $durable,
                $exclusive,
                $autoDelete,
                $deadLetterExchange,
                $deadLetterRoutingKey,
                $messageTtlMs
            ): array {
                $topology[self::QUEUES][$name] = [
                    self::URL => $url,
                    self::DURABLE => $durable,
                    self::EXCLUSIVE => $exclusive,
                    self::AUTO_DELETE => $autoDelete,
                    self::DEAD_LETTER_EXCHANGE => $deadLetterExchange,
                    self::DEAD_LETTER_ROUTING_KEY => $deadLetterRoutingKey,
                    self::MESSAGE_TTL_MS => $messageTtlMs,
                ];

                return $topology;
            }
        );
    }
}
