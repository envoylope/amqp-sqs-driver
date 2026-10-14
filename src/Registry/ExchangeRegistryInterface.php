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

/**
 * Interface ExchangeRegistryInterface.
 *
 * Store for AMQP exchange and queue topology, used to emulate
 * exchange routing on top of AWS SQS.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface ExchangeRegistryInterface
{
    public const ARGUMENTS = 'arguments';
    public const AUTO_DELETE = 'autoDelete';
    public const DEAD_LETTER_EXCHANGE = 'deadLetterExchange';
    public const DEAD_LETTER_ROUTING_KEY = 'deadLetterRoutingKey';
    public const DESTINATION_EXCHANGE = 'dest';
    public const DURABLE = 'durable';
    public const EXCHANGE_BINDINGS = 'exchangeBindings';
    public const EXCHANGE_TYPE = 'type';
    public const EXCHANGES = 'exchanges';
    public const EXCLUSIVE = 'exclusive';
    public const INTERNAL = 'internal';
    public const MESSAGE_TTL_MS = 'messageTtlMs';
    public const QUEUE = 'queue';
    public const QUEUE_BINDINGS = 'queueBindings';
    public const QUEUES = 'queues';
    public const ROUTING_KEY = 'routingKey';
    public const URL = 'url';

    /**
     * Stores a destination exchange binding on the source exchange.
     * When a message is published to the source, it is also forwarded to the destination.
     */
    public function addExchangeBinding(string $sourceExchangeName, string $destExchangeName, string $routingKey): void;

    /**
     * Stores a queue binding on the given exchange.
     * Idempotent: does not add a duplicate if the same binding already exists.
     */
    public function addQueueBinding(string $exchangeName, string $queueName, string $routingKey): void;

    /**
     * Deletes an exchange from the registry.
     */
    public function deleteExchange(string $name): void;

    /**
     * Deletes a queue from the registry.
     */
    public function deleteQueue(string $name): void;

    /**
     * Fetches the dead-letter exchange configured for the given AMQP queue name, or null if none is configured.
     *
     * The returned exchange's name is the target exchange name (or the empty string for the default exchange).
     */
    public function getDeadLetterExchange(string $queueName): ?DeadLetterExchangeInterface;

    /**
     * Fetches an exchange along with its stored topology, or null if not found.
     */
    public function getExchange(string $name): ?LiveLetterExchangeInterface;

    /**
     * Fetches the message TTL in milliseconds for the given AMQP queue name, or null if not set.
     */
    public function getQueueTtlMs(string $queueName): ?int;

    /**
     * Fetches the SQS queue URL for the given AMQP queue name, or null if not found.
     */
    public function getQueueUrl(string $queueName): ?string;

    /**
     * Removes a destination exchange binding from the source exchange.
     */
    public function removeExchangeBinding(
        string $sourceExchangeName,
        string $destExchangeName,
        string $routingKey
    ): void;

    /**
     * Removes a queue binding from the given exchange.
     */
    public function removeQueueBinding(string $exchangeName, string $queueName, string $routingKey): void;

    /**
     * Stores or updates an exchange in the registry, preserving any existing bindings.
     *
     * @param array<string, scalar> $arguments
     */
    public function setExchange(
        string $name,
        string $type,
        bool $durable,
        bool $autoDelete,
        bool $internal,
        array $arguments
    ): void;

    /**
     * Stores or updates a queue's SQS URL and metadata in the registry.
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
    ): void;
}
