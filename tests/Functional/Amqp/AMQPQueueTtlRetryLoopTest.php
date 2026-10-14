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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Functional\Amqp;

use AMQPChannel;
use AMQPConnection;
use AMQPEnvelope;
use AMQPExchange;
use AMQPQueue;
use Asmblah\PhpAmqpCompat\AmqpManager;
use Envoylope\Amqp\Driver\Sqs\Tests\Functional\AbstractFunctionalTestCase;

/**
 * Class AMQPQueueTtlRetryLoopTest.
 *
 * Tests the email/email.retry retry-loop workflow against a live ElasticMQ SQS server.
 *
 * When a queue is declared with both x-message-ttl and x-dead-letter-exchange, the driver
 * transparently redirects any publish to that queue through the named dead-letter exchange's
 * bound queue(s) with a SQS DelaySeconds equal to the TTL. This emulates the RabbitMQ TTL+DLX
 * pattern without requiring the caller to know about SQS.
 *
 * The dead-letter exchange is a real, separately-declared direct exchange (as in RabbitMQ) -
 * not a queue name - with the email queue bound to it under the dead-letter routing key.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class AMQPQueueTtlRetryLoopTest extends AbstractFunctionalTestCase
{
    private AMQPChannel $amqpChannel;
    private AMQPConnection $amqpConnection;
    private AMQPExchange $deadLetterExchange;
    private string $deadLetterExchangeName;
    private AMQPQueue $emailQueue;
    private string $emailQueueName;
    private AMQPQueue $retryQueue;
    private string $retryQueueName;
    private float $uniqueTestIdentifier;

    public function setUp(): void
    {
        parent::setUp();

        $this->resetAmqpManager();
        AmqpManager::setConfiguration($this->createConfiguration());

        $this->uniqueTestIdentifier = microtime(true);
        $this->emailQueueName = 'email-' . $this->uniqueTestIdentifier;
        $this->retryQueueName = 'email.retry-' . $this->uniqueTestIdentifier;
        $this->deadLetterExchangeName = 'email.dlx-' . $this->uniqueTestIdentifier;

        $this->connect();
    }

    public function tearDown(): void
    {
        parent::tearDown();

        $this->emailQueue->delete();
        $this->retryQueue->delete();

        $this->amqpConnection->disconnect();

        $this->resetAmqpManager();
    }

    /**
     * Validates the email/email.retry retry-loop workflow:
     *
     * 1. Worker publishes a failed email to email.retry with x-message-ttl=2000ms.
     * 2. The driver transparently redirects the publish to the email queue with
     *    SQS DelaySeconds=2, so the message is invisible for 2 seconds.
     * 3. After the delay elapses, the message becomes visible in the email queue
     *    and can be consumed for re-processing - no consumer on email.retry needed.
     */
    public function testMessagePublishedToRetryQueueAppearsInEmailQueueAfterTtlExpires(): void
    {
        $exchange = new AMQPExchange($this->amqpChannel);
        $exchange->setType(AMQP_EX_TYPE_DIRECT);

        // Publish to `email.retry` - the driver should redirect to `email` with 2s delay.
        $exchange->publish('retry-payload', $this->retryQueueName);

        // The message must not yet be visible in the email queue.
        $emailQueue = new AMQPQueue($this->amqpChannel);
        $emailQueue->setName($this->emailQueueName);
        $emailQueue->setFlags(AMQP_NOPARAM);
        $emailQueue->declareQueue();

        static::assertFalse(
            $emailQueue->get(),
            'Message should not yet be visible in email queue before the delay elapses'
        );

        // Wait for the SQS delivery delay to elapse.
        sleep(3);

        $emailQueue = new AMQPQueue($this->amqpChannel);
        $emailQueue->setName($this->emailQueueName);
        $emailQueue->setFlags(AMQP_NOPARAM);
        $emailQueue->declareQueue();

        $envelope = $emailQueue->get();
        static::assertInstanceOf(
            AMQPEnvelope::class,
            $envelope,
            'Message should be visible in email queue after the delay elapses'
        );
        static::assertSame('retry-payload', $envelope->getBody());
        $emailQueue->ack($envelope->getDeliveryTag());
    }

    public function testMetadataIsPreservedWhenMessageIsRedirectedViaRetryLoop(): void
    {
        $exchange = new AMQPExchange($this->amqpChannel);
        $exchange->setType(AMQP_EX_TYPE_DIRECT);

        $exchange->publish(
            'retry-with-metadata',
            $this->retryQueueName,
            AMQP_NOPARAM,
            [
                'content_type' => 'application/json',
                'correlation_id' => 'corr-456',
            ]
        );

        sleep(3);

        $emailQueue = new AMQPQueue($this->amqpChannel);
        $emailQueue->setName($this->emailQueueName);
        $emailQueue->setFlags(AMQP_NOPARAM);
        $emailQueue->declareQueue();

        $envelope = $emailQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope);
        static::assertSame('retry-with-metadata', $envelope->getBody());
        static::assertSame('application/json', $envelope->getContentType());
        static::assertSame('corr-456', $envelope->getCorrelationId());
        $emailQueue->ack($envelope->getDeliveryTag());
    }

    /**
     * Publishes two messages to a queue that has a DLX but no x-message-ttl, each with a
     * different per-message expiration. Verifies that the shorter-lived message appears in
     * the email queue first and the longer-lived message appears later, demonstrating that
     * different messages in the same queue can carry independent TTLs.
     */
    public function testMessagesWithDifferentPerMessageExpirationsArriveAtDifferentTimes(): void
    {
        // A plain retry queue: DLX only, no queue-level x-message-ttl.
        $plainRetryName = 'email.expiry-' . $this->uniqueTestIdentifier;

        $plainRetryQueue = new AMQPQueue($this->amqpChannel);
        $plainRetryQueue->setName($plainRetryName);
        $plainRetryQueue->setFlags(AMQP_NOPARAM);
        $plainRetryQueue->setArgument('x-dead-letter-exchange', $this->deadLetterExchangeName);
        $plainRetryQueue->setArgument('x-dead-letter-routing-key', $this->emailQueueName);
        $plainRetryQueue->declareQueue();

        $exchange = new AMQPExchange($this->amqpChannel);
        $exchange->setType(AMQP_EX_TYPE_DIRECT);

        // Message A: 2s expiration - driver redirects to `email` with DelaySeconds=2.
        $exchange->publish('fast', $plainRetryName, AMQP_NOPARAM, ['expiration' => '2000']);
        // Message B: 4s expiration - driver redirects to `email` with DelaySeconds=4.
        $exchange->publish('slow', $plainRetryName, AMQP_NOPARAM, ['expiration' => '4000']);

        // After 3 seconds only A's delay has elapsed.
        sleep(3);

        $emailQueue = new AMQPQueue($this->amqpChannel);
        $emailQueue->setName($this->emailQueueName);
        $emailQueue->setFlags(AMQP_NOPARAM);
        $emailQueue->declareQueue();

        $envelope = $emailQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope, '"fast" should be visible in email after 3s');
        static::assertSame('fast', $envelope->getBody());
        $emailQueue->ack($envelope->getDeliveryTag());

        static::assertFalse($emailQueue->get(), '"slow" should not yet be visible at 3s');

        // Wait for B's remaining delay to elapse (1s buffer on top of the 4s total).
        sleep(2);

        $emailQueue = new AMQPQueue($this->amqpChannel);
        $emailQueue->setName($this->emailQueueName);
        $emailQueue->setFlags(AMQP_NOPARAM);
        $emailQueue->declareQueue();

        $envelope = $emailQueue->get();
        static::assertInstanceOf(AMQPEnvelope::class, $envelope, '"slow" should be visible in email after 5s');
        static::assertSame('slow', $envelope->getBody());
        $emailQueue->ack($envelope->getDeliveryTag());

        // Clean up the extra queue (re-declare to get a handle, then delete).
        $cleanup = new AMQPQueue($this->amqpChannel);
        $cleanup->setName($plainRetryName);
        $cleanup->setFlags(AMQP_NOPARAM);
        $cleanup->setArgument('x-dead-letter-exchange', $this->deadLetterExchangeName);
        $cleanup->setArgument('x-dead-letter-routing-key', $this->emailQueueName);
        $cleanup->declareQueue();
        $cleanup->delete();
    }

    private function connect(): void
    {
        $this->amqpConnection = new AMQPConnection();
        $this->amqpConnection->connect();
        $this->amqpChannel = new AMQPChannel($this->amqpConnection);

        // Declare `email` (main) queue.
        $this->emailQueue = new AMQPQueue($this->amqpChannel);
        $this->emailQueue->setName($this->emailQueueName);
        $this->emailQueue->setFlags(AMQP_NOPARAM);
        $this->emailQueue->declareQueue();

        // Declare the dead-letter exchange as a real direct exchange, and bind the `email` queue to it
        // under the email queue's name as the dead-letter routing key.
        $this->deadLetterExchange = new AMQPExchange($this->amqpChannel);
        $this->deadLetterExchange->setName($this->deadLetterExchangeName);
        $this->deadLetterExchange->setType(AMQP_EX_TYPE_DIRECT);
        $this->deadLetterExchange->declareExchange();
        $this->emailQueue->bind($this->deadLetterExchangeName, $this->emailQueueName);

        // Declare email.retry queue with TTL and a DLX that routes back to email.
        $this->retryQueue = new AMQPQueue($this->amqpChannel);
        $this->retryQueue->setName($this->retryQueueName);
        $this->retryQueue->setFlags(AMQP_NOPARAM);
        $this->retryQueue->setArgument('x-message-ttl', 2000);
        $this->retryQueue->setArgument('x-dead-letter-exchange', $this->deadLetterExchangeName);
        $this->retryQueue->setArgument('x-dead-letter-routing-key', $this->emailQueueName);
        $this->retryQueue->declareQueue();
    }
}
