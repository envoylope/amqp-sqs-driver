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

namespace Envoylope\Amqp\Driver\Sqs\Heartbeat;

use Asmblah\PhpAmqpCompat\Bridge\Connection\AmqpConnectionBridgeInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Heartbeat\HeartbeatTransmitterInterface;
use Asmblah\PhpAmqpCompat\Scheduler\Heartbeat\HeartbeatSchedulerInterface;

/**
 * Class NullHeartbeatTransmitter.
 *
 * No-op heartbeat transmitter, as SQS does not require heartbeat keepalives.
 *
 * Note that if a heartbeat interval is configured,
 * it will still be taken into account for consumption long-polling; see Channel->wait(...).
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class NullHeartbeatTransmitter implements HeartbeatTransmitterInterface
{
    /**
     * @inheritDoc
     */
    public function transmit(
        HeartbeatSchedulerInterface $heartbeatScheduler,
        AmqpConnectionBridgeInterface $connectionBridge
    ): void {
        /*
         * SQS requires no periodic heartbeat; do nothing.
         *
         * Note that if a heartbeat interval is configured,
         * it will still be taken into account for consumption long-polling; see Channel->wait(...).
         */
    }
}
