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

namespace Envoylope\Amqp\Driver\Sqs\Transport;

use AMQPConnectionException;
use Asmblah\PhpAmqpCompat\Connection\Config\ConnectionConfigInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportConnectorInterface;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Channel\ChannelFactoryInterface;
use Exception;

/**
 * Class TransportConnector.
 *
 * Creates SQS Transport instances.
 * The AMQP ConnectionConfig is intentionally ignored; SQS connectivity is configured
 * via the SqsClient injected into SqsDriver.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class TransportConnector implements TransportConnectorInterface
{
    public function __construct(
        private readonly ChannelFactoryInterface $channelFactory,
        private readonly SqsClient $sqsClient
    ) {
    }

    /**
     * @inheritDoc
     */
    public function connect(ConnectionConfigInterface $config, string $methodName): TransportInterface
    {
        // Note that $config is unused for credentials, due to SQS credentials
        // being passed into SqsClient injected into SqsDriver.

        try {
            // Verify SQS connectivity with a lightweight request.
            $this->sqsClient->listQueues(['MaxResults' => 1]);
        } catch (Exception $exception) {
            throw new AMQPConnectionException(
                $methodName . '(): Could not connect to SQS: ' . $exception->getMessage(),
                previous: $exception
            );
        }

        return new Transport($this->channelFactory, $config->getHeartbeatInterval());
    }
}
