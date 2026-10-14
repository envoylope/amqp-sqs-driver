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

namespace Envoylope\Amqp\Driver\Sqs\Sleeper;

/**
 * Interface SleeperInterface.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface SleeperInterface
{
    /**
     * Blocks for the given number of microseconds.
     */
    public function sleep(int $microseconds): void;
}
