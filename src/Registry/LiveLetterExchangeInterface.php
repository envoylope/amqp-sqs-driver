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
 * Interface LiveLetterExchangeInterface.
 *
 * A declared AMQP exchange, along with its stored topology (type, arguments and bindings),
 * as emulated on top of AWS SQS.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface LiveLetterExchangeInterface extends ExchangeInterface
{
    /**
     * Fetches the exchange's declaration arguments.
     *
     * @return array<string, scalar>
     */
    public function getArguments(): array;

    /**
     * Fetches the exchange-to-exchange bindings stored on the exchange.
     *
     * @return list<ExchangeBindingInterface>
     */
    public function getExchangeBindings(): array;

    /**
     * Fetches the queue bindings stored on the exchange.
     *
     * @return list<QueueBindingInterface>
     */
    public function getQueueBindings(): array;

    /**
     * Fetches the exchange type, e.g. `AMQP_EX_TYPE_DIRECT`.
     */
    public function getType(): string;

    /**
     * Determines whether the exchange is deleted once no longer in use.
     */
    public function isAutoDelete(): bool;

    /**
     * Determines whether the exchange is durable.
     */
    public function isDurable(): bool;

    /**
     * Determines whether the exchange is internal (not publishable-to by clients).
     */
    public function isInternal(): bool;
}
