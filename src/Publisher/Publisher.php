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

namespace Envoylope\Amqp\Driver\Sqs\Publisher;

use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolverInterface;

/**
 * Class Publisher.
 *
 * Publishes a message to SQS, emulating AMQP message expiry/TTLs/dead-lettering.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Publisher implements PublisherInterface
{
    public function __construct(
        private readonly SqsClient $sqsClient,
        private readonly ExchangeRegistryInterface $registry,
        private readonly RouteResolverInterface $routeResolver,
        private readonly LoggerInterface $logger
    ) {
    }

    /**
     * @inheritDoc
     */
    public function sendToSqsQueue(
        string $queueUrl,
        string $queueName,
        string $body,
        array $amqpAttributes,
        string $exchangeName,
        string $routingKey,
        string $exceptionClass,
        string $methodName
    ): void {
        $queueTtlMs = $this->registry->getQueueTtlMs($queueName);
        $deadLetterExchange = $this->registry->getDeadLetterExchange($queueName);

        $expirationStr = (string) ($amqpAttributes['expiration'] ?? '');
        $messageTtlMs = $expirationStr !== '' ? (int) $expirationStr : null;

        if (($messageTtlMs !== null || $queueTtlMs !== null) && $deadLetterExchange === null) {
            $this->logger->warning(
                'message and/or queue TTL set but queue has no dead-letter exchange; TTL cannot be emulated and will be ignored',
                ['queueName' => $queueName, 'messageTtlMs' => $messageTtlMs, 'queueTtlMs' => $queueTtlMs]
            );
        }

        $effectiveTtlMs = match (true) {
            $messageTtlMs !== null && $queueTtlMs !== null => min($messageTtlMs, $queueTtlMs),
            $messageTtlMs !== null                         => $messageTtlMs,
            $queueTtlMs !== null                           => $queueTtlMs,
            default                                        => null,
        };

        // TTL & DLX: emulate RabbitMQ's server-side expiry by redirecting the message through the named
        // dead-letter exchange's bound queue(s) with an SQS delivery delay equal to the TTL. Without a
        // dead-letter exchange the TTL cannot be emulated, so the message is sent to its queue as normal.
        $redirectViaDeadLetterExchange = $effectiveTtlMs !== null && $deadLetterExchange !== null;

        if ($redirectViaDeadLetterExchange && $effectiveTtlMs > 900_000) {
            $this->logger->warning(
                'message TTL exceeds SQS maximum delivery delay of 900 seconds; clamping to 900s',
                ['queueName' => $queueName, 'ttlMs' => $effectiveTtlMs]
            );

            $effectiveTtlMs = 900_000;
        }

        try {
            $metadata = [
                'exchange' => $exchangeName,
                'routing_key' => $routingKey,
                'content_type' => $amqpAttributes['content_type'] ?? null,
                'content_encoding' => $amqpAttributes['content_encoding'] ?? null,
                'delivery_mode' => $amqpAttributes['delivery_mode'] ?? AMQP_DELIVERY_MODE_TRANSIENT,
                'priority' => $amqpAttributes['priority'] ?? 0,
                'correlation_id' => $amqpAttributes['correlation_id'] ?? null,
                'reply_to' => $amqpAttributes['reply_to'] ?? null,
                'expiration' => $amqpAttributes['expiration'] ?? null,
                'message_id' => $amqpAttributes['message_id'] ?? null,
                'timestamp' => $amqpAttributes['timestamp'] ?? null,
                'type' => $amqpAttributes['type'] ?? null,
                'user_id' => $amqpAttributes['user_id'] ?? null,
                'app_id' => $amqpAttributes['app_id'] ?? null,
                'cluster_id' => $amqpAttributes['cluster_id'] ?? null,
                'headers' => $amqpAttributes['headers'] ?? [],
            ];

            $messageAttributes = [
                'amqp_metadata' => [
                    'DataType' => 'String',
                    'StringValue' => json_encode($metadata, JSON_THROW_ON_ERROR),
                ],
            ];

            if ($redirectViaDeadLetterExchange) {
                $deadLetterRoutingKey = $deadLetterExchange->getRoutingKey() ?? $routingKey;
                $targetQueueUrls = $this->routeResolver->resolveQueueUrls(
                    $deadLetterExchange->getName(),
                    $deadLetterRoutingKey
                );

                if ($targetQueueUrls === []) {
                    $this->logger->warning(
                        'message TTL elapsed but the dead-letter exchange routed to no queue; message dropped',
                        [
                            'queueName' => $queueName,
                            'deadLetterExchange' => $deadLetterExchange->getName(),
                            'deadLetterRoutingKey' => $deadLetterRoutingKey,
                        ]
                    );

                    return;
                }

                foreach ($targetQueueUrls as $targetQueueUrl) {
                    $params = [
                        'QueueUrl' => $targetQueueUrl,
                        'MessageBody' => $body,
                        'MessageAttributes' => $messageAttributes,
                    ];

                    if ($effectiveTtlMs !== null && $effectiveTtlMs > 0) {
                        $params['DelaySeconds'] = (int) round($effectiveTtlMs / 1000);
                    }

                    $this->sqsClient->sendMessage($params);
                }

                return;
            }

            $this->sqsClient->sendMessage([
                'QueueUrl' => $queueUrl,
                'MessageBody' => $body,
                'MessageAttributes' => $messageAttributes,
            ]);
        } catch (SqsException $exception) {
            throw new $exceptionClass(
                $methodName . '(): Failed to publish message: ' . $exception->getMessage(),
                previous: $exception
            );
        }
    }
}
