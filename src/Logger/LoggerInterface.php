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

use Asmblah\PhpAmqpCompat\Driver\Common\Logger\LoggerInterface as AmqpCompatLoggerInterface;

/**
 * Interface LoggerInterface.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface LoggerInterface extends AmqpCompatLoggerInterface
{
}
