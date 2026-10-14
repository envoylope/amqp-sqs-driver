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

namespace Envoylope\Amqp\Driver\Sqs\Exception;

use Asmblah\PhpAmqpCompat\Driver\Common\Exception\ExceptionHandlerInterface;
use Exception;

/**
 * Class ExceptionHandler.
 *
 * Converts AWS SDK exceptions into the appropriate AMQPException subclasses.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ExceptionHandler implements ExceptionHandlerInterface
{
    /**
     * @inheritDoc
     */
    public function handleException(
        Exception $libraryException,
        string $exceptionClass,
        string $methodName,
        bool $isConsumption = false
    ): never {
        throw new $exceptionClass(
            $methodName . '(): ' . $libraryException->getMessage(),
            previous: $libraryException
        );
    }
}
