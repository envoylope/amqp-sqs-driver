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

use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;

/**
 * Class RouteResolver.
 *
 * Resolves an exchange and routing key to a set of destination SQS queues,
 * walking queue bindings and (recursively) exchange-to-exchange bindings.
 * Used both for ordinary publishes and for dead-letter-exchange routing.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class RouteResolver implements RouteResolverInterface
{
    public function __construct(
        private readonly ExchangeRegistryInterface $registry,
        private readonly BindingMatcherInterface $bindingMatcher
    ) {
    }

    /**
     * @inheritDoc
     */
    public function resolveQueueUrls(string $exchangeName, string $routingKey): array
    {
        return $this->resolve($exchangeName, $routingKey, []);
    }

    /**
     * @param array<string, true> $visitedExchanges Guards against exchange-to-exchange binding cycles.
     * @return array<string, string>
     */
    private function resolve(string $exchangeName, string $routingKey, array $visitedExchanges): array
    {
        if ($exchangeName === '') {
            // Default exchange (always direct): route to the queue named by the routing key.
            $url = $this->registry->getQueueUrl($routingKey);

            return $url !== null ? [$routingKey => $url] : [];
        }

        if (isset($visitedExchanges[$exchangeName])) {
            // Already visited during recursion, so we can skip.
            return [];
        }

        $visitedExchanges[$exchangeName] = true;

        $exchange = $this->registry->getExchange($exchangeName);

        if ($exchange === null) {
            // Exchange doesn't exist.
            return [];
        }

        $queueUrls = [];

        // Check exchange->queue bindings.
        foreach ($exchange->getQueueBindings() as $binding) {
            if (!$this->bindingMatcher->messageMatchesBinding($exchange->getType(), $binding->getRoutingKey(), $routingKey)) {
                continue;
            }

            $url = $this->registry->getQueueUrl($binding->getQueueName());

            if ($url !== null) {
                $queueUrls[$binding->getQueueName()] = $url;
            }
        }

        // Check exchange->exchange bindings (recursively, where the final recursion will need to be to a queue).
        foreach ($exchange->getExchangeBindings() as $exchangeBinding) {
            if (!$this->bindingMatcher->messageMatchesBinding($exchange->getType(), $exchangeBinding->getRoutingKey(), $routingKey)) {
                continue;
            }

            foreach ($this->resolve($exchangeBinding->getDestinationExchangeName(), $routingKey, $visitedExchanges) as $queueName => $url) {
                $queueUrls[$queueName] = $url;
            }
        }

        return $queueUrls;
    }
}
