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
 * Interface EnvelopeTransformerInterface.
 *
 * Builds the php-amqp-compat AMQPEnvelope handed to consumer callbacks and returned from
 * AMQPQueue::get(), restoring the AMQP properties from the JSON metadata attribute on the SQS message.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface EnvelopeTransformerInterface
{
    /**
     * Builds an AMQPEnvelope from an SQS-derived Envelope. A message whose metadata attribute is missing or
     * unparseable yields an envelope with only its body, delivery tag, consumer tag and redelivered flag.
     */
    public function transformEnvelope(EnvelopeInterface $envelope): AMQPEnvelope;
}
