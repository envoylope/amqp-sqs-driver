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

namespace Envoylope\Amqp\Driver\Sqs\Logger;

use Psr\Log\AbstractLogger as PsrAbstractLogger;
use Psr\Log\LoggerInterface as PsrLoggerInterface;
use Stringable;

/**
 * Class Logger.
 *
 * Wraps a PSR-3 logger, implementing the driver-level LoggerInterface.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Logger extends PsrAbstractLogger implements LoggerInterface
{
    public function __construct(
        private readonly PsrLoggerInterface $wrappedLogger
    ) {
    }

    /**
     * @inheritDoc
     */
    public function getWrappedLogger(): PsrLoggerInterface
    {
        return $this->wrappedLogger;
    }

    /**
     * @inheritDoc
     *
     * Note that $message is untyped for compatibility with psr/log v1 as well as v2+.
     *
     * @param Stringable|string $message
     * @param array<mixed> $context
     */
    public function log($level, $message, array $context = []): void
    {
        // Prefix all logs to make them consistently identifiable.
        $this->wrappedLogger->log($level, 'Envoylope AMQP SQS: ' . $message, $context);
    }
}
