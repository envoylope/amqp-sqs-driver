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
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolverInterface;

/**
 * Class DeadLetterHandler.
 *
 * Handles the process of dead-lettering a message, publishing it to a dead-letter exchange if applicable.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class DeadLetterHandler implements DeadLetterHandlerInterface
{
    public function __construct(
        private readonly SqsClient $sqsClient,
        private readonly ExchangeRegistryInterface $registry,
        private readonly RouteResolverInterface $routeResolver
    ) {
    }

    /**
     * @inheritDoc
     */
    public function deadLetter(EnvelopeInterface $envelope): void
    {
        $deadLetterExchange = $this->registry->getDeadLetterExchange($envelope->getQueueName());

        if ($deadLetterExchange === null) {
            // No dead-letter exchange was defined for the queue, so we cannot dead-letter the message;
            // instead, it is silently dropped.
            return;
        }

        $routingKey = $deadLetterExchange->getRoutingKey() ?? $envelope->getRoutingKey();

        $queueUrls = $this->routeResolver->resolveQueueUrls($deadLetterExchange->getName(), $routingKey);

        foreach ($queueUrls as $queueUrl) {
            $this->sqsClient->sendMessage([
                'QueueUrl' => $queueUrl,
                'MessageBody' => $envelope->getBody(),
                'MessageAttributes' => [
                    'amqp_metadata' => [
                        'DataType' => 'String',
                        'StringValue' => $envelope->getMetadataJson(),
                    ],
                ],
            ]);
        }
    }
}
