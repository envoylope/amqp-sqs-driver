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

namespace Envoylope\Amqp\Driver\Sqs\Transport;

use Asmblah\PhpAmqpCompat\Driver\Common\Channel\ChannelInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\ChannelFactoryInterface;

/**
 * Class Transport.
 *
 * Represents a logical connection to AWS SQS.
 * Note that SQS is stateless (each call is an independent HTTP request), so this class
 * tracks conceptual connection state rather than a real socket.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Transport implements TransportInterface
{
    private bool $connected = true;

    private int $nextChannelId = 1;

    public function __construct(
        private readonly ChannelFactoryInterface $channelFactory,
        private readonly int $heartbeatInterval
    ) {
    }

    /**
     * @inheritDoc
     */
    public function checkHeartbeat(): void
    {
        // SQS requires no heartbeats, as long-polling is used for consumption
        // and publishes are done via separate HTTP requests.

        // However, we do still use the heartbeat interval as the max long-polling time(out),
        // to ensure any silently dropped connection is always eventually detected.
    }

    /**
     * @inheritDoc
     */
    public function disconnect(string $exceptionClass, string $methodName): void
    {
        $this->connected = false;
    }

    /**
     * @inheritDoc
     */
    public function getHeartbeatInterval(): int
    {
        return $this->heartbeatInterval;
    }

    /**
     * @inheritDoc
     */
    public function isBusy(): bool
    {
        return false;
    }

    /**
     * @inheritDoc
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * @inheritDoc
     */
    public function openChannel(string $exceptionClass, string $methodName): ChannelInterface
    {
        if (!$this->connected) {
            throw new $exceptionClass($methodName . '(): Not connected to SQS.');
        }

        return $this->channelFactory->createChannel($this, $this->nextChannelId++);
    }

    /**
     * @inheritDoc
     */
    public function setReadTimeout(float $seconds): void
    {
        // No persistent socket to reconfigure; read timeout is respected in Channel::wait().
    }
}
