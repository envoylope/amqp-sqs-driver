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

namespace Envoylope\Amqp\Driver\Sqs\Routing;

/**
 * Interface RouteResolverInterface.
 *
 * Resolves an exchange and routing key to a set of destination SQS queues,
 * walking queue bindings and (recursively) exchange-to-exchange bindings.
 * Used both for ordinary publishes and for dead-letter-exchange routing.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
interface RouteResolverInterface
{
    /**
     * Resolves the exchange and routing key to the SQS queue URLs it delivers to.
     *
     * An empty exchange name denotes the default exchange, which routes to the
     * queue whose name equals the routing key. Unknown exchanges/queues and
     * binding cycles yield no route rather than an error.
     *
     * @return array<string, string> Map of AMQP queue name => SQS queue URL (empty if nothing matched).
     */
    public function resolveQueueUrls(string $exchangeName, string $routingKey): array;
}
