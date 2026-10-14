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
 * Interface MetadataParserInterface.
 *
 * Decodes the JSON `amqp_metadata` attribute carried on an SQS delivery back into an array of
 * AMQP envelope properties.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface MetadataParserInterface
{
    /**
     * Decodes the given AMQP metadata JSON, returning an empty array when it is absent or
     * unparseable (logging a warning in the unparseable case).
     *
     * @return array<string, mixed>
     */
    public function parse(string $metadataJson): array;
}
