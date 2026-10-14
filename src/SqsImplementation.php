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

namespace Envoylope\Amqp\Driver\Sqs;

use Asmblah\PhpAmqpCompat\Configuration\ConfigurationInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Exception\ExceptionHandlerInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Heartbeat\HeartbeatTransmitterInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportConnectorInterface;
use Asmblah\PhpAmqpCompat\Driver\ImplementationInterface;
use Asmblah\PhpAmqpCompat\Misc\Clock;
use Asmblah\PhpAmqpCompat\Misc\ClockInterface;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandler;
use Envoylope\Amqp\Driver\Sqs\Bridge\MetadataParser;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspector;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolver;
use Envoylope\Amqp\Driver\Sqs\Bridge\Sanitiser;
use Envoylope\Amqp\Driver\Sqs\Channel\ChannelFactory;
use Envoylope\Amqp\Driver\Sqs\Exception\ExceptionHandler;
use Envoylope\Amqp\Driver\Sqs\Heartbeat\NullHeartbeatTransmitter;
use Envoylope\Amqp\Driver\Sqs\Logger\Logger;
use Envoylope\Amqp\Driver\Sqs\Publisher\Publisher;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcher;
use Envoylope\Amqp\Driver\Sqs\Routing\RouteResolver;
use Envoylope\Amqp\Driver\Sqs\Routing\TopicMatcher;
use Envoylope\Amqp\Driver\Sqs\Routing\TopicPartMatcher;
use Envoylope\Amqp\Driver\Sqs\Sleeper\Sleeper;
use Envoylope\Amqp\Driver\Sqs\Sleeper\SleeperInterface;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformer;
use Envoylope\Amqp\Driver\Sqs\Transport\TransportConnector;

/**
 * Class SqsImplementation.
 *
 * Wires together the SQS driver components.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class SqsImplementation implements ImplementationInterface
{
    private readonly ExceptionHandlerInterface $exceptionHandler;
    private readonly HeartbeatTransmitterInterface $heartbeatTransmitter;
    private readonly Logger $logger;
    private readonly TransportConnectorInterface $transportConnector;

    public function __construct(
        SqsClient $sqsClient,
        ExchangeRegistryInterface $registry,
        ConfigurationInterface $configuration,
        int $maxConsumptionLongPollDuration,
        int $messageVisibilityTimeoutSeconds,
        ClockInterface $clock = new Clock(),
        SleeperInterface $sleeper = new Sleeper()
    ) {
        $this->exceptionHandler = new ExceptionHandler();
        $this->heartbeatTransmitter = new NullHeartbeatTransmitter();
        $this->logger = new Logger($configuration->getLogger());
        $bindingMatcher = new BindingMatcher(new TopicMatcher(new TopicPartMatcher()));
        $routeResolver = new RouteResolver($registry, $bindingMatcher);
        $deadLetterHandler = new DeadLetterHandler(
            $sqsClient,
            $registry,
            $routeResolver
        );
        $sanitiser = new Sanitiser();
        $channelFactory = new ChannelFactory(
            $sqsClient,
            $registry,
            $bindingMatcher,
            $deadLetterHandler,
            new Publisher($sqsClient, $registry, $routeResolver, $this->logger),
            new EnvelopeTransformer(),
            new MetadataParser($this->logger),
            new QueueInspector($sqsClient, $this->logger),
            new QueueUrlResolver($registry, $sqsClient, $this->logger, $sanitiser),
            $sanitiser,
            $this->logger,
            $clock,
            $sleeper,
            $maxConsumptionLongPollDuration,
            $messageVisibilityTimeoutSeconds
        );
        $this->transportConnector = new TransportConnector($channelFactory, $sqsClient);
    }

    /**
     * @inheritDoc
     */
    public function getExceptionHandler(): ExceptionHandlerInterface
    {
        return $this->exceptionHandler;
    }

    /**
     * @inheritDoc
     */
    public function getHeartbeatTransmitter(): HeartbeatTransmitterInterface
    {
        return $this->heartbeatTransmitter;
    }

    /**
     * @inheritDoc
     */
    public function getLogger(): Logger
    {
        return $this->logger;
    }

    /**
     * @inheritDoc
     */
    public function getTransportConnector(): TransportConnectorInterface
    {
        return $this->transportConnector;
    }
}
