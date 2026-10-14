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

use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;

/**
 * Interface DeadLetterHandlerInterface.
 *
 * Handles the process of dead-lettering a message, publishing it to a dead-letter exchange if applicable.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface DeadLetterHandlerInterface
{
    /**
     * Dead-letters the given message envelope.
     */
    public function deadLetter(EnvelopeInterface $envelope): void;
}
