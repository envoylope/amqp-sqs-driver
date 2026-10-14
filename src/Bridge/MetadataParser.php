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

use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use JsonException;

/**
 * Class MetadataParser.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class MetadataParser implements MetadataParserInterface
{
    public function __construct(
        private readonly LoggerInterface $logger
    ) {
    }

    /**
     * @inheritDoc
     */
    public function parse(string $metadataJson): array
    {
        try {
            /** @var array<string, mixed> $metadata */
            $metadata = json_decode($metadataJson, true, 512, JSON_THROW_ON_ERROR) ?: [];
        } catch (JsonException $exception) {
            $this->logger->warning('Failed to decode AMQP metadata from message attribute; falling back to empty metadata', [
                'invalid_json' => $metadataJson,
                'exception' => $exception,
            ]);

            return [];
        }

        return $metadata;
    }
}
