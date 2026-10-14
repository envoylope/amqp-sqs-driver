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

namespace Envoylope\Amqp\Driver\Sqs\Bridge;

/**
 * Interface QueueInspectorInterface.
 *
 * Reads runtime attributes of an SQS queue that AMQP callers expect to be told about
 * (e.g. the message count returned from AMQPQueue::declareQueue()).
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface QueueInspectorInterface
{
    /**
     * Fetches the queue's approximate message count, returning 0 (and logging a warning) if SQS
     * cannot be reached. The count is advisory only.
     */
    public function fetchApproximateMessageCount(string $queueUrl): int;
}
