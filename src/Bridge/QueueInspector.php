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

use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Exception;

/**
 * Class QueueInspector.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class QueueInspector implements QueueInspectorInterface
{
    public function __construct(
        private readonly SqsClient $sqsClient,
        private readonly LoggerInterface $logger
    ) {
    }

    /**
     * @inheritDoc
     */
    public function fetchApproximateMessageCount(string $queueUrl): int
    {
        try {
            $attrs = $this->sqsClient->getQueueAttributes([
                'QueueUrl' => $queueUrl,
                'AttributeNames' => ['ApproximateNumberOfMessages'],
            ]);

            return (int) ($attrs->get('Attributes')['ApproximateNumberOfMessages'] ?? 0);
        } catch (Exception $exception) {
            $this->logger->warning('Failed to fetch approximate message count', [
                'queueUrl' => $queueUrl,
                'exception' => $exception,
            ]);

            return 0;
        }
    }
}
