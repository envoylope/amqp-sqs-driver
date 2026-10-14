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
 * Class Sleeper.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Sleeper implements SleeperInterface
{
    /**
     * @inheritDoc
     */
    public function sleep(int $microseconds): void
    {
        usleep($microseconds);
    }
}
