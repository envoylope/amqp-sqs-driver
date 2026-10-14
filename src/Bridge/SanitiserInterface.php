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
 * Interface SanitiserInterface.
 *
 * Sanitises AMQP queue names into valid SQS names.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface SanitiserInterface
{
    /**
     * Converts an AMQP queue name to a valid SQS queue name (alphanumerics, hyphens and underscores
     * only, at most 80 characters - over-long names are truncated with a hash suffix).
     */
    public function sanitiseQueueName(string $name): string;
}
