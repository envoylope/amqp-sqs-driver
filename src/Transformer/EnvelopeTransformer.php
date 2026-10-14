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

namespace Envoylope\Amqp\Driver\Sqs\Transformer;

use AMQPEnvelope;
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;

/**
 * Class EnvelopeTransformer.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class EnvelopeTransformer implements EnvelopeTransformerInterface
{
    /**
     * @inheritDoc
     */
    public function transformEnvelope(EnvelopeInterface $envelope): AMQPEnvelope
    {
        $metadata = $envelope->getMetadata();

        /** @var array<string, mixed> $headers */
        $headers = $metadata['headers'] ?? [];

        return new AMQPEnvelope(
            $envelope->getBody(),
            $envelope->getConsumerTag(),
            $envelope->getDeliveryTag(),
            $metadata['exchange'] ?? '',
            $envelope->isRedelivered(),
            $metadata['routing_key'] ?? '',
            $metadata['content_type'] ?? null,
            $metadata['content_encoding'] ?? null,
            $headers,
            (int) ($metadata['delivery_mode'] ?? AMQP_DELIVERY_MODE_TRANSIENT),
            (int) ($metadata['priority'] ?? 0),
            (string) ($metadata['correlation_id'] ?? ''),
            (string) ($metadata['reply_to'] ?? ''),
            (string) ($metadata['expiration'] ?? ''),
            (string) ($metadata['message_id'] ?? ''),
            (int) ($metadata['timestamp'] ?? 0),
            (string) ($metadata['type'] ?? ''),
            (string) ($metadata['user_id'] ?? ''),
            (string) ($metadata['app_id'] ?? ''),
            (string) ($metadata['cluster_id'] ?? '')
        );
    }
}
