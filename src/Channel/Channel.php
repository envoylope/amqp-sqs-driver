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

namespace Envoylope\Amqp\Driver\Sqs\Channel;

use AMQPEnvelope;
use Asmblah\PhpAmqpCompat\Driver\Common\Channel\ChannelInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Asmblah\PhpAmqpCompat\Misc\ClockInterface;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandlerInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspectorInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolverInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\SanitiserInterface;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Publisher\PublisherInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Sleeper\SleeperInterface;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformerInterface;
use Exception;

/**
 * Class Channel.
 *
 * Implements the AMQP channel interface using AWS SQS for message transport.
 * Exchanges are emulated via an ExchangeRegistry. Each AMQP queue is instead backed by an SQS one.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class Channel implements ChannelInterface
{
    /**
     * @var array<string, ConsumerInterface>
     */
    private array $consumersByTag = [];
    /**
     * @var array<int, EnvelopeInterface>
     */
    private array $envelopesByDeliveryTag = [];
    private int $globalPrefetchCount = 0;
    private int $localPrefetchCount = 0;
    private int $nextDeliveryTag = 1;
    private bool $open = true;

    public function __construct(
        private readonly ChannelFactoryInterface $channelFactory,
        private readonly TransportInterface $transport,
        private readonly SqsClient $sqsClient,
        private readonly ExchangeRegistryInterface $registry,
        private readonly BindingMatcherInterface $bindingMatcher,
        private readonly DeadLetterHandlerInterface $deadLetterHandler,
        private readonly PublisherInterface $publisher,
        private readonly EnvelopeTransformerInterface $envelopeTransformer,
        private readonly QueueInspectorInterface $queueInspector,
        private readonly QueueUrlResolverInterface $queueUrlResolver,
        private readonly SanitiserInterface $sanitiser,
        private readonly int $channelId,
        private readonly LoggerInterface $logger,
        private readonly ClockInterface $clock,
        private readonly SleeperInterface $sleeper,
        private readonly int $maxConsumptionLongPollDuration,
        private readonly int $messageVisibilityTimeoutSeconds,
        // Default: 10 microseconds. How long to pause the busy-wait in ->wait() each time the
        // channel-global prefetch window is full, so we do not spin the CPU while waiting
        // for an ack/nack to free up space.
        private readonly int $prefetchFullWaitMicroseconds = 10
    ) {
    }

    /**
     * @inheritDoc
     */
    public function basicAck(int $deliveryTag, bool $multiple, string $exceptionClass, string $methodName): void
    {
        if (!$multiple) {
            $envelope = $this->envelopesByDeliveryTag[$deliveryTag] ?? null;

            if ($envelope === null) {
                throw new $exceptionClass(
                    $methodName . '(): Unknown delivery tag: ' . $deliveryTag
                );
            }

            try {
                $this->sqsClient->deleteMessage([
                    'QueueUrl' => $envelope->getQueueUrl(),
                    'ReceiptHandle' => $envelope->getReceiptHandle(),
                ]);
            } catch (Exception $exception) {
                throw new $exceptionClass(
                    $methodName . '(): Failed to ack message: ' . $exception->getMessage(),
                    previous: $exception
                );
            }

            $envelope->acknowledge();
            unset($this->envelopesByDeliveryTag[$deliveryTag]);

            return;
        }

        // Multiple flag: acknowledge all messages up to and including the given delivery tag.

        /** @var array<int, array{int, Exception}> $failures */
        $failures = [];

        foreach ($this->envelopesByDeliveryTag as $tag => $envelope) {
            if ($tag <= $deliveryTag) {
                try {
                    $this->sqsClient->deleteMessage([
                        'QueueUrl' => $envelope->getQueueUrl(),
                        'ReceiptHandle' => $envelope->getReceiptHandle(),
                    ]);
                } catch (Exception $exception) {
                    $this->logger->warning($methodName . '(): Failed to ack message (tag ' . $tag . ')', [
                        'exception' => $exception,
                    ]);

                    $failures[] = [$tag, $exception];
                }

                $envelope->acknowledge();
                unset($this->envelopesByDeliveryTag[$tag]);
            }
        }

        if (!empty($failures)) {
            $message = count($failures) === 1
                ? $methodName . '(): Failed to ack message (tag ' . $failures[0][0] . '): ' . $failures[0][1]->getMessage()
                : $methodName . '(): Failed to ack ' . count($failures) . ' messages: ' . implode(', ', array_map(
                    static fn (array $failure) => 'tag ' . $failure[0] . ' (' . $failure[1]->getMessage() . ')',
                    $failures
                ));

            throw new $exceptionClass($message, previous: $failures[0][1]);
        }
    }

    /**
     * @inheritDoc
     */
    public function basicCancel(
        string $consumerTag,
        bool $noWait,
        string $exceptionClass,
        string $methodName
    ): void {
        unset($this->consumersByTag[$consumerTag]);
    }

    /**
     * @inheritDoc
     */
    public function basicConsume(
        string $queueName,
        string $consumerTag,
        bool $noLocal,
        bool $autoAck,
        bool $exclusive,
        callable $callback,
        string $exceptionClass,
        string $methodName
    ): string {
        if ($consumerTag === '') {
            // We always need a tag for the consumer, so generate one if needed.
            do {
                $consumerTag = 'ctag-' . bin2hex(random_bytes(8));
            } while (array_key_exists($consumerTag, $this->consumersByTag));
        }

        $queueUrl = $this->queueUrlResolver->resolve($queueName, $exceptionClass, $methodName);

        $this->consumersByTag[$consumerTag] = $this->channelFactory->createConsumer(
            $consumerTag,
            $queueName,
            $queueUrl,
            $callback(...),
            $autoAck,
            $this->localPrefetchCount
        );

        return $consumerTag;
    }

    /**
     * @inheritDoc
     */
    public function basicGet(
        string $queueName,
        bool $autoAck,
        string $exceptionClass,
        string $methodName
    ): ?AMQPEnvelope {
        $queueUrl = $this->queueUrlResolver->resolve($queueName, $exceptionClass, $methodName);

        // TODO: Handle OverLimit (when >~120000 messages have been fetched
        //       but not yet reached their visibility timeout) by returning null?
        try {
            $result = $this->sqsClient->receiveMessage([
                'QueueUrl' => $queueUrl,
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => 0,
                'MessageAttributeNames' => ['All'],
                'MessageSystemAttributeNames' => ['ApproximateReceiveCount'],
                // If the application fails to acknowledge the message before the configured timeout
                // it will be requeued, leading to duplicate work if the message is still being handled.
                'VisibilityTimeout' => $this->messageVisibilityTimeoutSeconds,
            ]);
        } catch (Exception $exception) {
            throw new $exceptionClass(
                $methodName . '(): Failed to receive message from queue "' . $queueName . '": ' . $exception->getMessage(),
                previous: $exception
            );
        }

        $messages = $result->get('Messages') ?? [];

        if (empty($messages)) {
            return null;
        }

        $sqsMessage = $messages[0];
        $redelivered = (int) ($sqsMessage['Attributes']['ApproximateReceiveCount'] ?? 1) > 1;
        $deliveryTag = $this->nextDeliveryTag++;

        $envelope = $this->channelFactory->createEnvelope(
            $deliveryTag,
            $sqsMessage['ReceiptHandle'],
            $queueName,
            $queueUrl,
            $sqsMessage['Body'],
            $sqsMessage['MessageAttributes']['amqp_metadata']['StringValue'] ?? '{}',
            $redelivered,
            consumer: null
        );
        $amqpEnvelope = $this->envelopeTransformer->transformEnvelope($envelope);

        if ($autoAck) {
            try {
                $this->sqsClient->deleteMessage([
                    'QueueUrl' => $queueUrl,
                    'ReceiptHandle' => $sqsMessage['ReceiptHandle'],
                ]);
            } catch (Exception $exception) {
                throw new $exceptionClass(
                    $methodName . '(): Failed to auto-ack message: ' . $exception->getMessage(),
                    previous: $exception
                );
            }
        } else {
            $this->envelopesByDeliveryTag[$deliveryTag] = $envelope;
        }

        return $amqpEnvelope;
    }

    /**
     * @inheritDoc
     */
    public function basicNack(
        int $deliveryTag,
        bool $multiple,
        bool $requeue,
        string $exceptionClass,
        string $methodName
    ): void {
        $tagsToProcess = $multiple
            // Multiple flag: negatively-acknowledge all messages up to and including the given delivery tag.
            ? array_filter(array_keys($this->envelopesByDeliveryTag), static fn (int $tag) => $tag <= $deliveryTag)
            : [$deliveryTag];

        /** @var array<int, array{int, Exception}> $failures */
        $failures = [];

        foreach ($tagsToProcess as $tag) {
            $envelope = $this->envelopesByDeliveryTag[$tag] ?? null;

            if ($envelope === null) {
                continue;
            }

            try {
                if ($requeue) {
                    // Make the message immediately re-available for redelivery.
                    $this->sqsClient->changeMessageVisibility([
                        'QueueUrl' => $envelope->getQueueUrl(),
                        'ReceiptHandle' => $envelope->getReceiptHandle(),
                        'VisibilityTimeout' => 0,
                    ]);
                } else {
                    $this->deadLetterHandler->deadLetter($envelope);

                    $this->sqsClient->deleteMessage([
                        'QueueUrl' => $envelope->getQueueUrl(),
                        'ReceiptHandle' => $envelope->getReceiptHandle(),
                    ]);
                }
            } catch (Exception $exception) {
                if ($multiple) {
                    $this->logger->warning($methodName . '(): Failed to nack message (tag ' . $tag . ')', [
                        'exception' => $exception,
                    ]);
                    $failures[] = [$tag, $exception];
                } else {
                    throw new $exceptionClass(
                        $methodName . '(): Failed to nack message: ' . $exception->getMessage(),
                        previous: $exception
                    );
                }
            }

            $envelope->acknowledge();
            unset($this->envelopesByDeliveryTag[$tag]);
        }

        if ($failures !== []) {
            $message = count($failures) === 1
                ? $methodName . '(): Failed to nack message (tag ' . $failures[0][0] . '): ' . $failures[0][1]->getMessage()
                : $methodName . '(): Failed to nack ' . count($failures) . ' messages: ' . implode(', ', array_map(
                    static fn (array $f) => 'tag ' . $f[0] . ' (' . $f[1]->getMessage() . ')',
                    $failures
                ));

            throw new $exceptionClass($message, previous: $failures[0][1]);
        }
    }

    /**
     * @inheritDoc
     */
    public function basicPublish(
        string $message,
        array $attributes,
        string $exchangeName,
        string $routingKey,
        bool $mandatory,
        bool $immediate,
        string $exceptionClass,
        string $methodName
    ): void {
        if ($exchangeName === '') {
            // Default exchange: route directly to the SQS queue named by the routing key.
            $queueUrl = $this->queueUrlResolver->resolve($routingKey, $exceptionClass, $methodName);
            $this->publisher->sendToSqsQueue($queueUrl, $routingKey, $message, $attributes, $exchangeName, $routingKey, $exceptionClass, $methodName);

            return;
        }

        $exchange = $this->registry->getExchange($exchangeName);

        if ($exchange === null) {
            if ($mandatory) {
                throw new $exceptionClass($methodName . '(): Exchange not found: ' . $exchangeName);
            }

            return;
        }

        // Route to directly-bound queues.
        foreach ($exchange->getQueueBindings() as $binding) {
            if ($this->bindingMatcher->messageMatchesBinding($exchange->getType(), $binding->getRoutingKey(), $routingKey)) {
                $queueUrl = $this->registry->getQueueUrl($binding->getQueueName());

                if ($queueUrl !== null) {
                    $this->publisher->sendToSqsQueue(
                        $queueUrl,
                        $binding->getQueueName(),
                        $message,
                        $attributes,
                        $exchangeName,
                        $routingKey,
                        $exceptionClass,
                        $methodName
                    );
                }
            }
        }

        // Follow exchange-to-exchange bindings (recursive).
        foreach ($exchange->getExchangeBindings() as $exchangeBinding) {
            if ($this->bindingMatcher->messageMatchesBinding($exchange->getType(), $exchangeBinding->getRoutingKey(), $routingKey)) {
                $this->basicPublish(
                    $message,
                    $attributes,
                    $exchangeBinding->getDestinationExchangeName(),
                    $routingKey,
                    false,
                    false,
                    $exceptionClass,
                    $methodName
                );
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function basicQos(
        int $size,
        int $count,
        bool $global,
        string $exceptionClass,
        string $methodName
    ): void {
        if ($global) {
            $this->globalPrefetchCount = $count;
        } else {
            $this->localPrefetchCount = $count;
        }
    }

    /**
     * @inheritDoc
     */
    public function basicRecover(bool $requeue, string $exceptionClass, string $methodName): void
    {
        /** @var array<int, array{int, Exception}> $failures */
        $failures = [];

        foreach ($this->envelopesByDeliveryTag as $tag => $envelope) {
            try {
                if ($requeue) {
                    // Make immediately re-available.
                    $this->sqsClient->changeMessageVisibility([
                        'QueueUrl' => $envelope->getQueueUrl(),
                        'ReceiptHandle' => $envelope->getReceiptHandle(),
                        'VisibilityTimeout' => 0,
                    ]);
                } else {
                    $this->sqsClient->deleteMessage([
                        'QueueUrl' => $envelope->getQueueUrl(),
                        'ReceiptHandle' => $envelope->getReceiptHandle(),
                    ]);
                }
            } catch (Exception $exception) {
                $this->logger->warning($methodName . '(): Failed to recover message (tag ' . $tag . ')', [
                    'exception' => $exception,
                ]);
                $failures[] = [$tag, $exception];
            }

            $envelope->acknowledge();
            unset($this->envelopesByDeliveryTag[$tag]);
        }

        if ($failures !== []) {
            $message = count($failures) === 1
                ? $methodName . '(): Failed to recover message (tag ' . $failures[0][0] . '): ' . $failures[0][1]->getMessage()
                : $methodName . '(): Failed to recover ' . count($failures) . ' messages: ' . implode(', ', array_map(
                    static fn (array $f) => 'tag ' . $f[0] . ' (' . $f[1]->getMessage() . ')',
                    $failures
                ));

            throw new $exceptionClass($message, previous: $failures[0][1]);
        }
    }

    /**
     * @inheritDoc
     */
    public function basicReject(
        int $deliveryTag,
        bool $requeue,
        string $exceptionClass,
        string $methodName
    ): void {
        $envelope = $this->envelopesByDeliveryTag[$deliveryTag] ?? null;

        if ($envelope === null) {
            return;
        }

        try {
            if ($requeue) {
                $this->sqsClient->changeMessageVisibility([
                    'QueueUrl' => $envelope->getQueueUrl(),
                    'ReceiptHandle' => $envelope->getReceiptHandle(),
                    'VisibilityTimeout' => 0,
                ]);
            } else {
                $this->deadLetterHandler->deadLetter($envelope);

                $this->sqsClient->deleteMessage([
                    'QueueUrl' => $envelope->getQueueUrl(),
                    'ReceiptHandle' => $envelope->getReceiptHandle(),
                ]);
            }
        } catch (Exception $exception) {
            throw new $exceptionClass(
                $methodName . '(): Failed to reject message: ' . $exception->getMessage(),
                previous: $exception
            );
        }

        $envelope->acknowledge();
        unset($this->envelopesByDeliveryTag[$deliveryTag]);
    }

    /**
     * @inheritDoc
     */
    public function bindExchange(
        string $destinationExchangeName,
        string $sourceExchangeName,
        string $routingKey,
        bool $noWait,
        array $arguments,
        string $exceptionClass,
        string $methodName
    ): void {
        $this->registry->addExchangeBinding($sourceExchangeName, $destinationExchangeName, $routingKey);
    }

    /**
     * @inheritDoc
     */
    public function bindQueue(
        string $queueName,
        string $exchangeName,
        string $routingKey,
        array $arguments,
        string $exceptionClass,
        string $methodName
    ): void {
        $this->registry->addQueueBinding($exchangeName, $queueName, $routingKey);
    }

    /**
     * @inheritDoc
     */
    public function closeQuietly(): void
    {
        $this->open = false;
    }

    /**
     * @inheritDoc
     */
    public function commitTransaction(string $exceptionClass, string $methodName): void
    {
        // SQS does not support AMQP-style transactions; this is a no-op.

        // TODO: Consider (perhaps behind a flag) buffering locally in-memory to emulate.
    }

    /**
     * @inheritDoc
     */
    public function declareExchange(
        string $exchangeName,
        string $exchangeType,
        bool $passive,
        bool $durable,
        bool $autoDelete,
        bool $internal,
        bool $noWait,
        array $arguments,
        string $exceptionClass,
        string $methodName
    ): void {
        if ($passive) {
            if ($this->registry->getExchange($exchangeName) === null) {
                throw new $exceptionClass(
                    $methodName . '(): Exchange not found: ' . $exchangeName
                );
            }

            return;
        }

        $this->registry->setExchange(
            $exchangeName,
            $exchangeType,
            $durable,
            $autoDelete,
            $internal,
            $arguments
        );
    }

    /**
     * @inheritDoc
     */
    public function declareQueue(
        string $queueName,
        bool $passive,
        bool $durable,
        bool $exclusive,
        bool $autoDelete,
        bool $noWait,
        array $arguments,
        string $exceptionClass,
        string $methodName
    ): array {
        if ($queueName === '') {
            // Auto-generate a unique queue name.
            $sqsQueueName = 'amqp-' . bin2hex(random_bytes(8));
            $actualQueueName = $sqsQueueName;
        } else {
            $sqsQueueName = $this->sanitiser->sanitiseQueueName($queueName);
            $actualQueueName = $queueName;
        }

        if ($passive) {
            // Only check existence; do not create.
            $existingUrl = $this->registry->getQueueUrl($actualQueueName);

            if ($existingUrl === null) {
                try {
                    $result = $this->sqsClient->getQueueUrl(['QueueName' => $sqsQueueName]);
                    $existingUrl = $result->get('QueueUrl');
                } catch (Exception $exception) {
                    throw new $exceptionClass(
                        $methodName . '(): Queue does not exist: ' . $queueName,
                        previous: $exception
                    );
                }
            }

            $count = $this->queueInspector->fetchApproximateMessageCount($existingUrl);

            return ['name' => $actualQueueName, 'count' => $count];
        }

        // Create or confirm the SQS queue (createQueue is idempotent).
        try {
            $result = $this->sqsClient->createQueue(['QueueName' => $sqsQueueName]);
            $queueUrl = $result->get('QueueUrl');
        } catch (Exception $exception) {
            throw new $exceptionClass(
                $methodName . '(): Failed to create SQS queue "' . $sqsQueueName . '": ' . $exception->getMessage(),
                previous: $exception
            );
        }

        // Record the dead-letter exchange (if any). Unlike RabbitMQ there is no native SQS feature e.g. RedrivePolicy
        // involved: dead-lettering is emulated entirely through the exchange topology, resolved at the
        // moment a message is rejected/nacked without requeue or expires via TTL. This keeps the
        // behaviour faithful - RabbitMQ's x-dead-letter-exchange has no "after N deliveries" trigger.
        $deadLetterExchange = isset($arguments['x-dead-letter-exchange'])
            ? (string) $arguments['x-dead-letter-exchange']
            : null;
        $deadLetterRoutingKey = isset($arguments['x-dead-letter-routing-key'])
            ? (string) $arguments['x-dead-letter-routing-key']
            : null;

        $messageTtlMs = isset($arguments['x-message-ttl']) ? (int) $arguments['x-message-ttl'] : null;

        $this->registry->setQueue(
            $actualQueueName,
            $queueUrl,
            $durable,
            $exclusive,
            $autoDelete,
            $deadLetterExchange,
            $deadLetterRoutingKey,
            $messageTtlMs
        );

        $count = $this->queueInspector->fetchApproximateMessageCount($queueUrl);

        return ['name' => $actualQueueName, 'count' => $count];
    }

    /**
     * @inheritDoc
     */
    public function deleteExchange(
        string $exchangeName,
        bool $ifUnused,
        bool $noWait,
        string $exceptionClass,
        string $methodName
    ): void {
        $this->registry->deleteExchange($exchangeName);
    }

    /**
     * @inheritDoc
     */
    public function deleteQueue(
        string $queueName,
        bool $ifUnused,
        bool $ifEmpty,
        bool $noWait,
        string $exceptionClass,
        string $methodName
    ): int {
        $queueUrl = $this->queueUrlResolver->resolveOrNull($queueName);

        if ($queueUrl === null) {
            return 0;
        }

        $count = $this->queueInspector->fetchApproximateMessageCount($queueUrl);

        try {
            $this->sqsClient->deleteQueue(['QueueUrl' => $queueUrl]);
        } catch (Exception $exception) {
            throw new $exceptionClass(
                $methodName . '(): Failed to delete queue "' . $queueName . '": ' . $exception->getMessage(),
                previous: $exception
            );
        }

        $this->registry->deleteQueue($queueName);

        return $count;
    }

    /**
     * @inheritDoc
     */
    public function getChannelId(): ?int
    {
        if (!$this->open) {
            return null;
        }

        return $this->channelId;
    }

    /**
     * Fetches the configured global prefetch count.
     */
    public function getGlobalPrefetchCount(): int
    {
        return $this->globalPrefetchCount;
    }

    /**
     * Fetches the configured local prefetch count.
     */
    public function getLocalPrefetchCount(): int
    {
        return $this->localPrefetchCount;
    }

    /**
     * @inheritDoc
     */
    public function hasConnection(): bool
    {
        // The channel always has a connection object as long as the transport was created.
        return true;
    }

    /**
     * @inheritDoc
     */
    public function isConnected(): bool
    {
        return $this->open && $this->transport->isConnected();
    }

    /**
     * @inheritDoc
     */
    public function isOpen(): bool
    {
        return $this->open && $this->transport->isConnected();
    }

    /**
     * @inheritDoc
     */
    public function purgeQueue(
        string $queueName,
        bool $noWait,
        string $exceptionClass,
        string $methodName
    ): void {
        $queueUrl = $this->registry->getQueueUrl($queueName);

        if ($queueUrl === null) {
            throw new $exceptionClass(
                $methodName . '(): Queue not found: ' . $queueName
            );
        }

        // Drain the queue by receiving and deleting in batches of 10.
        do {
            try {
                $result = $this->sqsClient->receiveMessage([
                    'QueueUrl' => $queueUrl,
                    'MaxNumberOfMessages' => 10,
                    'WaitTimeSeconds' => 0,
                ]);
            } catch (Exception $exception) {
                throw new $exceptionClass(
                    $methodName . '(): Failed to purge queue "' . $queueName . '": ' . $exception->getMessage(),
                    previous: $exception
                );
            }

            $messages = $result->get('Messages') ?? [];

            if (!empty($messages)) {
                $entries = [];

                foreach ($messages as $index => $msg) {
                    $entries[] = ['Id' => (string) $index, 'ReceiptHandle' => $msg['ReceiptHandle']];
                }

                try {
                    $this->sqsClient->deleteMessageBatch([
                        'QueueUrl' => $queueUrl,
                        'Entries' => $entries,
                    ]);
                } catch (Exception $exception) {
                    throw new $exceptionClass(
                        $methodName . '(): Failed to delete message batch during purge of "' . $queueName . '": ' .
                            $exception->getMessage(),
                        previous: $exception
                    );
                }
            }
        } while (!empty($messages));
    }

    /**
     * @inheritDoc
     */
    public function rollbackTransaction(string $exceptionClass, string $methodName): void
    {
        // SQS does not support AMQP-style transactions; this is a no-op.
    }

    /**
     * @inheritDoc
     */
    public function startTransaction(string $exceptionClass, string $methodName): void
    {
        // SQS does not support AMQP-style transactions; this is a no-op.
    }

    /**
     * @inheritDoc
     */
    public function unbindExchange(
        string $destinationExchangeName,
        string $sourceExchangeName,
        string $routingKey,
        bool $noWait,
        array $arguments,
        string $exceptionClass,
        string $methodName
    ): void {
        $this->registry->removeExchangeBinding($sourceExchangeName, $destinationExchangeName, $routingKey);
    }

    /**
     * @inheritDoc
     */
    public function wait(float|int $timeout, string $exceptionClass, string $methodName): void
    {
        $heartbeatInterval = $this->transport->getHeartbeatInterval();
        $effectiveTimeout = (float) $timeout !== 0.0 ? $timeout : PHP_FLOAT_MAX;
        $deadline = $this->clock->getUnixTimestamp() + $effectiveTimeout;

        $maxConsumptionLongPollDuration = min(
            $effectiveTimeout,
            // If a heartbeat interval was set, it is intended to be configured based on intermediary proxies etc.,
            // so use it as a cap on the timeout.
            (float) $heartbeatInterval !== 0.0 ? $heartbeatInterval : PHP_FLOAT_MAX,
            $this->maxConsumptionLongPollDuration,
            20 // SQS has a hard limit of 20 seconds for long polling.
        );

        while (true) {
            if ($this->clock->getUnixTimestamp() >= $deadline) {
                // This message is as per the reference implementation.
                throw new $exceptionClass('Consumer timeout exceed');
            }

            // Keep track of how many messages are consumed this tick, so we can decide
            // whether to push the deadline back.
            $consumedMessages = 0;

            foreach ($this->consumersByTag as $consumerTag => $consumer) {
                if (!$consumer->prefetchQueueHasSpace()) {
                    // Consumer (local) prefetch queue is full - try next consumer.
                    continue;
                }

                $maxMessagesForChannel = $this->globalPrefetchCount === 0 ?
                    PHP_INT_MAX : // Unlimited.
                    max(0, $this->globalPrefetchCount - count($this->envelopesByDeliveryTag));

                if ($maxMessagesForChannel === 0) {
                    // Global prefetch queue is full, so there is no point in trying further consumers.
                    // Wait until either the timeout expires or a message is acked/nacked,
                    // which will free up space.
                    $this->sleeper->sleep($this->prefetchFullWaitMicroseconds);
                    break;
                }

                $maxMessagesForConsumer = $consumer->getAvailablePrefetchCount() ?? PHP_INT_MAX;
                $maxMessages = min($maxMessagesForChannel, $maxMessagesForConsumer);

                $remaining = $deadline - $this->clock->getUnixTimestamp();
                $pollDuration = (int) min($maxConsumptionLongPollDuration, max(0, floor($remaining)));

                try {
                    $result = $this->sqsClient->receiveMessage([
                        'QueueUrl' => $consumer->getQueueUrl(),
                        'MaxNumberOfMessages' => $maxMessages,
                        'WaitTimeSeconds' => $pollDuration,
                        'MessageAttributeNames' => ['All'],
                        'MessageSystemAttributeNames' => ['ApproximateReceiveCount'],
                        // If the application fails to acknowledge the message before the configured timeout
                        // it will be requeued, leading to duplicate work if the message is still being handled.
                        'VisibilityTimeout' => $this->messageVisibilityTimeoutSeconds,
                    ]);
                } catch (Exception $exception) {
                    throw new $exceptionClass(
                        $methodName . '(): Failed to receive messages: ' . $exception->getMessage(),
                        previous: $exception
                    );
                }

                $messages = $result->get('Messages') ?? [];

                foreach ($messages as $sqsMessage) {
                    $redelivered = (int) ($sqsMessage['Attributes']['ApproximateReceiveCount'] ?? 1) > 1;
                    $deliveryTag = $this->nextDeliveryTag++;

                    $envelope = $this->channelFactory->createEnvelope(
                        $deliveryTag,
                        $sqsMessage['ReceiptHandle'],
                        $consumer->getQueueName(),
                        $consumer->getQueueUrl(),
                        $sqsMessage['Body'],
                        $sqsMessage['MessageAttributes']['amqp_metadata']['StringValue'] ?? '{}',
                        $redelivered,
                        $consumer
                    );

                    if ($consumer->isAutoAck()) {
                        try {
                            $this->sqsClient->deleteMessage([
                                'QueueUrl' => $consumer->getQueueUrl(),
                                'ReceiptHandle' => $sqsMessage['ReceiptHandle'],
                            ]);
                        } catch (Exception $exception) {
                            throw new $exceptionClass(
                                $methodName . '(): Failed to auto-ack message: ' . $exception->getMessage(),
                                previous: $exception
                            );
                        }
                    } else {
                        $consumer->addUnacknowledgedEnvelope($envelope);
                        $this->envelopesByDeliveryTag[$deliveryTag] = $envelope;
                    }

                    $consumedMessages++;

                    $amqpEnvelope = $this->envelopeTransformer->transformEnvelope($envelope);

                    // May throw StopConsumptionException if the user callback returns false.
                    $consumer->getCallback()($amqpEnvelope);

                    if (!$consumer->prefetchQueueHasSpace()) {
                        // Consumer (local) prefetch queue is now full - try next consumer.
                        break;
                    }
                }
            }

            if ($consumedMessages > 0) {
                // Some work was done, so we can push the timeout deadline back.
                $deadline = $this->clock->getUnixTimestamp() + $effectiveTimeout;
            }
        }
    }
}
