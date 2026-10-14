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

namespace Envoylope\Amqp\Driver\Sqs\Setup;

use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Topology\TopologyStorageInterface;

/**
 * Interface SetupInterface.
 *
 * Provides the driver setup.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface SetupInterface
{
    /**
     * Fetches the maximum number of seconds a consumption long poll can take.
     */
    public function getMaxConsumptionLongPollDurationSeconds(): int;

    /**
     * Fetches the SQS client.
     */
    public function getSqsClient(): SqsClient;

    /**
     * Fetches the topology storage.
     */
    public function getTopologyStorage(): TopologyStorageInterface;

    /**
     * If the application fails to acknowledge a message before this timeout
     * it will be requeued, leading to duplicate work if the message is still being handled.
     */
    public function getVisibilityTimeoutSeconds(): int;
}
