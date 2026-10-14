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

namespace Envoylope\Amqp\Driver\Sqs\Registry;

/**
 * Interface ExchangeInterface.
 *
 * Common contract for the exchange value objects returned by ExchangeRegistryInterface.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface ExchangeInterface
{
    /**
     * Fetches the name of the exchange (or the empty string for the default exchange).
     */
    public function getName(): string;
}
