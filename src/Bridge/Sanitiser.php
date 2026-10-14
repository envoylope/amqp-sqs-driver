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
 * Class Sanitiser.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Sanitiser implements SanitiserInterface
{
    /**
     * @inheritDoc
     */
    public function sanitiseQueueName(string $name): string
    {
        $sanitized = preg_replace('/[^a-zA-Z0-9_-]/', '_', $name);

        if (strlen($sanitized) <= 80) {
            return $sanitized;
        }

        // Truncate with a hash suffix to stay within 80 characters.
        return substr($sanitized, 0, 47) . '_' . md5($name);
    }
}
