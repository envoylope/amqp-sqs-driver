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
 * Class ExistingSetup.
 *
 * Provides the driver setup with a fixed, existing client and storage.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ExistingSetup implements SetupInterface
{
    public function __construct(
        private readonly SqsClient $sqsClient,
        private readonly TopologyStorageInterface $topologyStorage,
        private readonly int $maxConsumptionLongPollDuration = 60 * 60,
        // If the application fails to acknowledge a message before this timeout
        // it will be requeued, leading to duplicate work if the message is still being handled.
        private readonly int $visibilityTimeoutSeconds = 60
    ) {
    }

    /**
     * @inheritDoc
     */
    public function getMaxConsumptionLongPollDurationSeconds(): int
    {
        return $this->maxConsumptionLongPollDuration;
    }

    /**
     * @inheritDoc
     */
    public function getSqsClient(): SqsClient
    {
        return $this->sqsClient;
    }

    /**
     * @inheritDoc
     */
    public function getTopologyStorage(): TopologyStorageInterface
    {
        return $this->topologyStorage;
    }

    /**
     * @inheritDoc
     */
    public function getVisibilityTimeoutSeconds(): int
    {
        return $this->visibilityTimeoutSeconds;
    }
}
