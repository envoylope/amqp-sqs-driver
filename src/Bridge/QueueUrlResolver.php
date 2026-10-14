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

use Aws\Exception\AwsException;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Exception;

/**
 * Class QueueUrlResolver.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class QueueUrlResolver implements QueueUrlResolverInterface
{
    public function __construct(
        private readonly ExchangeRegistryInterface $registry,
        private readonly SqsClient $sqsClient,
        private readonly LoggerInterface $logger,
        private readonly SanitiserInterface $sanitiser
    ) {
    }

    /**
     * @inheritDoc
     */
    public function resolve(string $queueName, string $exceptionClass, string $methodName): string
    {
        $url = $this->resolveOrNull($queueName);

        if ($url === null) {
            throw new $exceptionClass($methodName . '(): Queue not found: ' . $queueName);
        }

        return $url;
    }

    /**
     * @inheritDoc
     */
    public function resolveOrNull(string $queueName): ?string
    {
        $url = $this->registry->getQueueUrl($queueName);

        if ($url !== null) {
            return $url;
        }

        // Fall back to querying SQS directly.
        try {
            $result = $this->sqsClient->getQueueUrl(['QueueName' => $this->sanitiser->sanitiseQueueName($queueName)]);

            return $result->get('QueueUrl');
        } catch (AwsException $exception) {
            if ($exception->getAwsErrorCode() !== 'QueueDoesNotExist') {
                $this->logger->warning('Unexpected error resolving queue URL; treating queue as not found', [
                    'queueName' => $queueName,
                    'errorCode' => $exception->getAwsErrorCode(),
                    'exception' => $exception,
                ]);
            }

            return null;
        } catch (Exception $exception) {
            $this->logger->warning('Failed to resolve queue URL', [
                'queueName' => $queueName,
                'exception' => $exception,
            ]);

            return null;
        }
    }
}
