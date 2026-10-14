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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Channel;

use AMQPChannelException;
use AMQPEnvelope;
use AMQPExchangeException;
use AMQPQueueException;
use Asmblah\PhpAmqpCompat\Driver\Common\Transport\TransportInterface;
use Asmblah\PhpAmqpCompat\Exception\StopConsumptionException;
use Asmblah\PhpAmqpCompat\Misc\ClockInterface;
use Aws\Result;
use Aws\Sqs\SqsClient;
use Closure;
use Envoylope\Amqp\Driver\Sqs\Bridge\DeadLetterHandlerInterface;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueInspector;
use Envoylope\Amqp\Driver\Sqs\Bridge\QueueUrlResolver;
use Envoylope\Amqp\Driver\Sqs\Bridge\SanitiserInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\Channel;
use Envoylope\Amqp\Driver\Sqs\Channel\ChannelFactoryInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\ConsumerInterface;
use Envoylope\Amqp\Driver\Sqs\Channel\EnvelopeInterface;
use Envoylope\Amqp\Driver\Sqs\Logger\LoggerInterface;
use Envoylope\Amqp\Driver\Sqs\Publisher\PublisherInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistryInterface;
use Envoylope\Amqp\Driver\Sqs\Registry\LiveLetterExchange;
use Envoylope\Amqp\Driver\Sqs\Registry\QueueBinding;
use Envoylope\Amqp\Driver\Sqs\Routing\BindingMatcherInterface;
use Envoylope\Amqp\Driver\Sqs\Sleeper\SleeperInterface;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Transformer\EnvelopeTransformer;
use Exception;
use Mockery;
use Mockery\MockInterface;

/**
 * Class ChannelTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ChannelTest extends AbstractTestCase
{
    private MockInterface&BindingMatcherInterface $bindingMatcher;
    private Channel $channel;
    private MockInterface&ChannelFactoryInterface $channelFactory;
    private MockInterface&ClockInterface $clock;
    private MockInterface&ExchangeRegistryInterface $registry;
    private MockInterface&LoggerInterface $logger;
    private MockInterface&DeadLetterHandlerInterface $deadLetterHandler;
    private MockInterface&PublisherInterface $publisher;
    private MockInterface&SanitiserInterface $sanitiser;
    private MockInterface&SleeperInterface $sleeper;
    private MockInterface&SqsClient $sqsClient;
    private MockInterface&TransportInterface $transport;

    public function setUp(): void
    {
        $this->bindingMatcher = mock(BindingMatcherInterface::class);
        $this->channelFactory = mock(ChannelFactoryInterface::class);
        $this->clock = mock(ClockInterface::class);
        $this->logger = mock(LoggerInterface::class);
        $this->publisher = mock(PublisherInterface::class);
        $this->deadLetterHandler = mock(DeadLetterHandlerInterface::class);
        $this->registry = mock(ExchangeRegistryInterface::class, [
            'getQueueTtlMs' => null,
        ]);
        $this->sanitiser = mock(SanitiserInterface::class);
        $this->sqsClient = mock(SqsClient::class)
            // Needed due to the `@method`-annotation approach of SqsClient.
            ->shouldAllowMockingMethod('changeMessageVisibility')
            ->shouldAllowMockingMethod('createQueue')
            ->shouldAllowMockingMethod('deleteMessage')
            ->shouldAllowMockingMethod('getQueueAttributes')
            ->shouldAllowMockingMethod('getQueueUrl')
            ->shouldAllowMockingMethod('receiveMessage')
            ->shouldAllowMockingMethod('sendMessage')
            ->shouldAllowMockingMethod('setQueueAttributes');
        $this->transport = mock(TransportInterface::class, [
            'getHeartbeatInterval' => 0,
        ]);
        $this->sleeper = mock(SleeperInterface::class, [
            'sleep' => null,
        ]);

        $this->channelFactory->allows('createConsumer')
            ->andReturnUsing(function (
                string $consumerTag,
                string $queueName,
                string $queueUrl,
                Closure $callback,
                bool $autoAck,
                int $prefetchCount
            ) {
                return mock(ConsumerInterface::class, [
                    'addUnacknowledgedEnvelope' => null,
                    'getAvailablePrefetchCount' => $prefetchCount,
                    'getCallback' => $callback,
                    'getConsumerTag' => $consumerTag,
                    'getQueueName' => $queueName,
                    'getQueueUrl' => $queueUrl,
                    'isAutoAck' => $autoAck,
                    'prefetchQueueHasSpace' => true,
                ]);
            })
            ->byDefault();
        $this->channelFactory->allows('createEnvelope')
            ->andReturnUsing(function (
                int $deliveryTag,
                string $receiptHandle,
                string $queueName,
                string $queueUrl,
                string $body,
                string $metadataJson,
                bool $redelivered,
                ?ConsumerInterface $consumer
            ) {
                /** @var array<string, mixed> $metadata */
                $metadata = json_decode($metadataJson, true) ?: [];

                return mock(EnvelopeInterface::class, [
                    'acknowledge' => null,
                    'getBody' => $body,
                    'getConsumerTag' => $consumer?->getConsumerTag(),
                    'getDeliveryTag' => $deliveryTag,
                    'getMetadata' => $metadata,
                    'getMetadataJson' => $metadataJson,
                    'getQueueName' => $queueName,
                    'getQueueUrl' => $queueUrl,
                    'getReceiptHandle' => $receiptHandle,
                    'isRedelivered' => $redelivered,
                ]);
            })
            ->byDefault();
        $this->sanitiser->allows('sanitiseQueueName')
            ->andReturnUsing(fn (string $name) => '[sanitised]' . $name . '[/sanitised]')
            ->byDefault();

        $envelopeTransformer = new EnvelopeTransformer();
        $queueInspector = new QueueInspector($this->sqsClient, $this->logger);
        $queueUrlResolver = new QueueUrlResolver($this->registry, $this->sqsClient, $this->logger, $this->sanitiser);

        $this->channel = new Channel(
            $this->channelFactory,
            $this->transport,
            $this->sqsClient,
            $this->registry,
            $this->bindingMatcher,
            $this->deadLetterHandler,
            $this->publisher,
            $envelopeTransformer,
            $queueInspector,
            $queueUrlResolver,
            $this->sanitiser,
            42,
            $this->logger,
            $this->clock,
            $this->sleeper,
            120,
            100
        );
    }

    public function testBasicAckDeletesMessageFromSqsForSingleTag(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [
                $this->buildSqsMessage('hello', 'receipt-abc'),
            ],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-abc'])
            ->once();

        $this->channel->basicAck(1, false, AMQPQueueException::class, 'AMQPQueue::ack');
    }

    public function testBasicAckThrowsForUnknownDeliveryTag(): void
    {
        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('AMQPQueue::ack(): Unknown delivery tag: 99');

        $this->channel->basicAck(99, false, AMQPQueueException::class, 'AMQPQueue::ack');
    }

    public function testBasicAckWithMultipleFlagDeletesAllTagsUpToAndIncluding(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        // Each basicGet() call gets its own response so the receipt handles are distinct.
        $this->sqsClient->allows('receiveMessage')->andReturn(
            new Result(['Messages' => [$this->buildSqsMessage('msg1', 'receipt-1')]]),
            new Result(['Messages' => [$this->buildSqsMessage('msg2', 'receipt-2')]]),
            new Result(['Messages' => [$this->buildSqsMessage('msg3', 'receipt-3')]])
        );
        // Fetch three messages to register delivery tags 1, 2, 3.
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-1'])
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-2'])
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-3'])
            ->never();

        $this->channel->basicAck(2, true, AMQPQueueException::class, 'AMQPQueue::ack');
    }

    public function testBasicAckThrowsOnSqsDeleteFailure(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('msg', 'receipt-xyz')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->sqsClient->allows('deleteMessage')
            ->andThrow(new Exception('SQS error'));

        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('AMQPQueue::ack(): Failed to ack message: SQS error');

        $this->channel->basicAck(1, false, AMQPQueueException::class, 'AMQPQueue::ack');
    }

    public function testBasicCancelRemovesRegisteredConsumer(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->clock->allows('getUnixTimestamp')->andReturn(1000, 1030);
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function () {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->channel->basicCancel('my-tag', false, AMQPQueueException::class, 'AMQPQueue::cancel');

        // After cancellation, the consumer must not be polled during wait().
        $this->sqsClient->expects('receiveMessage')
            ->never();
        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testBasicConsumeRegistersConsumerAndReturnsProvidedTag(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        $result = $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            static function () {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );

        static::assertSame('my-tag', $result);
    }

    public function testBasicConsumeAutoGeneratesTagWhenEmpty(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        $result = $this->channel->basicConsume(
            'my_queue',
            '',
            false,
            false,
            false,
            static function () {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );

        static::assertMatchesRegularExpression('/^ctag-[0-9a-f]{16}$/', $result);
    }

    public function testBasicConsumeThrowsWhenQueueNotFound(): void
    {
        $this->registry->allows('getQueueUrl')->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')->andThrow(new Exception('not found'));
        $this->logger->allows('warning');

        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('AMQPQueue::consume(): Queue not found: unknown_queue');

        $this->channel->basicConsume(
            'unknown_queue',
            '',
            false,
            false,
            false,
            static function () {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
    }

    public function testBasicGetReturnsNullWhenQueueIsEmpty(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result(['Messages' => []]));

        $result = $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        static::assertNull($result);
    }

    public function testBasicGetReturnsEnvelopeWhenMessageIsAvailable(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [
                $this->buildSqsMessage('hello world', 'receipt-123', [
                    'exchange' => 'my_exchange',
                    'routing_key' => 'my_key',
                ]),
            ],
        ]));

        $envelope = $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        static::assertInstanceOf(AMQPEnvelope::class, $envelope);
        static::assertSame('hello world', $envelope->getBody());
        static::assertSame('my_exchange', $envelope->getExchangeName());
        static::assertSame('my_key', $envelope->getRoutingKey());
        static::assertSame(1, $envelope->getDeliveryTag());
        static::assertFalse($envelope->isRedelivery());
    }

    public function testBasicGetMarksMessageAsRedeliveredWhenApproximateReceiveCountIsAboveOne(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [
                $this->buildSqsMessage('hello', 'receipt-redelivered', [], receiveCount: 3),
            ],
        ]));

        $envelope = $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        static::assertInstanceOf(AMQPEnvelope::class, $envelope);
        static::assertTrue($envelope->isRedelivery());
    }

    public function testBasicGetAutoAcksMessageWhenFlagTrue(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-autoack')],
        ]));

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-autoack'])
            ->once();

        $this->channel->basicGet('my_queue', true, AMQPQueueException::class, 'AMQPQueue::get');
    }

    public function testBasicGetDoesNotAutoAckWhenFlagFalse(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-noautoack')],
        ]));

        $this->sqsClient->expects('deleteMessage')
            ->never();

        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
    }

    public function testBasicGetThrowsOnSqsReceiveFailure(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andThrow(new Exception('Network error'));

        $this->expectException(AMQPQueueException::class);
        $this->expectExceptionMessage('AMQPQueue::get(): Failed to receive message from queue "my_queue": Network error');

        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
    }

    public function testBasicNackWithRequeueMakesMessageImmediatelyVisible(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-nack')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->sqsClient->expects('changeMessageVisibility')
            ->with([
                'QueueUrl' => 'https://sqs.example.com/my_queue',
                'ReceiptHandle' => 'receipt-nack',
                'VisibilityTimeout' => 0,
            ])
            ->once();

        $this->channel->basicNack(1, false, true, AMQPQueueException::class, 'AMQPQueue::nack');
    }

    public function testBasicNackWithoutRequeueDeletesMessage(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-nack-delete')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->deadLetterHandler->allows('deadLetter');

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-nack-delete'])
            ->once();

        $this->channel->basicNack(1, false, false, AMQPQueueException::class, 'AMQPQueue::nack');
    }

    public function testBasicPublishSendsToDefaultExchangeQueueDirectlyViaPublisher(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        $this->publisher->expects('sendToSqsQueue')
            ->with(
                'https://sqs.example.com/my_queue',
                'my_queue',
                'hello',
                [],
                '',
                'my_queue',
                AMQPExchangeException::class,
                'AMQPExchange::publish'
            )
            ->once();

        $this->channel->basicPublish(
            'hello',
            [],
            '',
            'my_queue',
            false,
            false,
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testBasicPublishThrowsWhenDefaultExchangeQueueCannotBeResolved(): void
    {
        $this->registry->allows()
            ->getQueueUrl('missing_queue')
            ->andReturnNull();
        $this->sqsClient->allows('getQueueUrl')->andThrow(new Exception('not found'));
        $this->logger->allows('warning');

        $this->publisher->expects('sendToSqsQueue')
            ->never();
        $this->expectException(AMQPExchangeException::class);
        $this->expectExceptionMessage('AMQPExchange::publish(): Queue not found: missing_queue');

        $this->channel->basicPublish(
            'hello',
            [],
            '',
            'missing_queue',
            false,
            false,
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testBasicPublishRoutesViaDirectExchangeToMatchingBoundQueue(): void
    {
        $this->registry->allows()
            ->getExchange('my_exchange')
            ->andReturn(new LiveLetterExchange(
                'my_exchange',
                AMQP_EX_TYPE_DIRECT,
                true,
                false,
                false,
                [],
                [
                    new QueueBinding('my_queue', 'my_key'),
                    new QueueBinding('other_queue', 'other_key'),
                ],
                []
            ));
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'my_key', 'my_key')
            ->andReturnTrue();
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_DIRECT, 'other_key', 'my_key')
            ->andReturnFalse();

        $this->publisher->expects('sendToSqsQueue')
            ->with(
                'https://sqs.example.com/my_queue',
                'my_queue',
                'hello',
                [],
                'my_exchange',
                'my_key',
                AMQPExchangeException::class,
                'AMQPExchange::publish'
            )
            ->once();

        $this->channel->basicPublish(
            'hello',
            [],
            'my_exchange',
            'my_key',
            false,
            false,
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testBasicPublishFanoutExchangeRoutesToAllBoundQueues(): void
    {
        $this->registry->allows()
            ->getExchange('my_fanout')
            ->andReturn(new LiveLetterExchange(
                'my_fanout',
                AMQP_EX_TYPE_FANOUT,
                true,
                false,
                false,
                [],
                [
                    new QueueBinding('queue_a', ''),
                    new QueueBinding('queue_b', ''),
                ],
                []
            ));
        $this->registry->allows()
            ->getQueueUrl('queue_a')
            ->andReturn('https://sqs.example.com/queue_a');
        $this->registry->allows()
            ->getQueueUrl('queue_b')
            ->andReturn('https://sqs.example.com/queue_b');
        $this->bindingMatcher->allows()
            ->messageMatchesBinding(AMQP_EX_TYPE_FANOUT, '', '')
            ->andReturnTrue();

        $this->publisher->expects('sendToSqsQueue')
            ->with(
                'https://sqs.example.com/queue_a',
                'queue_a',
                'hello',
                [],
                'my_fanout',
                '',
                AMQPExchangeException::class,
                'AMQPExchange::publish'
            )
            ->once();
        $this->publisher->expects('sendToSqsQueue')
            ->with(
                'https://sqs.example.com/queue_b',
                'queue_b',
                'hello',
                [],
                'my_fanout',
                '',
                AMQPExchangeException::class,
                'AMQPExchange::publish'
            )
            ->once();

        $this->channel->basicPublish(
            'hello',
            [],
            'my_fanout',
            '',
            false,
            false,
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testBasicPublishThrowsForMissingExchangeWhenMandatory(): void
    {
        $this->registry->allows('getExchange')->andReturnNull();

        $this->expectException(AMQPExchangeException::class);
        $this->expectExceptionMessage('AMQPExchange::publish(): Exchange not found: nonexistent');

        $this->channel->basicPublish(
            'hello',
            [],
            'nonexistent',
            'key',
            true,
            false,
            AMQPExchangeException::class,
            'AMQPExchange::publish'
        );
    }

    public function testBasicQosSetsInternalPrefetchCountUsedDuringWait(): void
    {
        $this->channel->basicQos(0, 5, false, AMQPChannelException::class, 'AMQPChannel::qos');

        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function (AMQPEnvelope $envelope): void {
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('deleteMessage')->andReturn(new Result([]));

        $this->sqsClient->expects('receiveMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(5, $args['MaxNumberOfMessages']);

                return new Result([
                    'Messages' => [$this->buildSqsMessage('hello', 'receipt-qos')],
                ]);
            });
        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testBasicRejectWithRequeueMakesMessageImmediatelyVisible(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-reject')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->sqsClient->expects('changeMessageVisibility')
            ->with([
                'QueueUrl' => 'https://sqs.example.com/my_queue',
                'ReceiptHandle' => 'receipt-reject',
                'VisibilityTimeout' => 0,
            ])
            ->once();

        $this->channel->basicReject(1, true, AMQPQueueException::class, 'AMQPQueue::reject');
    }

    public function testBasicRejectWithoutRequeuDeletesMessage(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-reject-delete')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->deadLetterHandler->allows('deadLetter');

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-reject-delete'])
            ->once();

        $this->channel->basicReject(1, false, AMQPQueueException::class, 'AMQPQueue::reject');
    }

    public function testBasicRecoverWithRequeueMakesAllPendingMessagesImmediatelyVisible(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [
                $this->buildSqsMessage('msg1', 'receipt-recover-1'),
                $this->buildSqsMessage('msg2', 'receipt-recover-2'),
            ],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->sqsClient->expects('changeMessageVisibility')
            ->twice()
            ->andReturn(new Result([]));

        $this->channel->basicRecover(true, AMQPChannelException::class, 'AMQPChannel::basicRecover');
    }

    public function testBindQueueDelegatesToRegistry(): void
    {
        $this->registry->expects('addQueueBinding')
            ->with('my_exchange', 'my_queue', 'my_key')
            ->once();

        $this->channel->bindQueue(
            'my_queue',
            'my_exchange',
            'my_key',
            [],
            AMQPQueueException::class,
            'AMQPQueue::bind'
        );
    }

    public function testBindExchangeDelegatesToRegistry(): void
    {
        $this->registry->expects('addExchangeBinding')
            ->with('src_exchange', 'dest_exchange', 'my_key')
            ->once();

        $this->channel->bindExchange(
            'dest_exchange',
            'src_exchange',
            'my_key',
            false,
            [],
            AMQPExchangeException::class,
            'AMQPExchange::bind'
        );
    }

    public function testUnbindExchangeDelegatesToRegistry(): void
    {
        $this->registry->expects('removeExchangeBinding')
            ->with('src_exchange', 'dest_exchange', 'my_key')
            ->once();

        $this->channel->unbindExchange(
            'dest_exchange',
            'src_exchange',
            'my_key',
            false,
            [],
            AMQPExchangeException::class,
            'AMQPExchange::unbind'
        );
    }

    public function testCloseQuietlyMakesGetChannelIdReturnNull(): void
    {
        static::assertSame(42, $this->channel->getChannelId());

        $this->channel->closeQuietly();

        static::assertNull($this->channel->getChannelId());
    }

    public function testGetChannelIdReturnsIdWhenOpen(): void
    {
        static::assertSame(42, $this->channel->getChannelId());
    }

    /**
     * @dataProvider booleanDataProvider
     */
    public function testIsConnectedDelegatesToTransportWhenOpen(bool $connected): void
    {
        $this->transport->allows('isConnected')->andReturn($connected);

        static::assertSame($connected, $this->channel->isConnected());
    }

    public function testIsConnectedReturnsFalseWhenClosedRegardlessOfTransport(): void
    {
        $this->transport->allows('isConnected')->andReturnTrue();
        $this->channel->closeQuietly();

        static::assertFalse($this->channel->isConnected());
    }

    /**
     * @dataProvider booleanDataProvider
     */
    public function testIsOpenDelegatesToTransportWhenOpen(bool $connected): void
    {
        $this->transport->allows('isConnected')->andReturn($connected);

        static::assertSame($connected, $this->channel->isOpen());
    }

    public function testIsOpenReturnsFalseAfterCloseQuietly(): void
    {
        $this->transport->allows('isConnected')->andReturnTrue();
        $this->channel->closeQuietly();

        static::assertFalse($this->channel->isOpen());
    }

    public function testWaitThrowsTimeoutExceptionOnceDeadlineIsReached(): void
    {
        // No consumers - the loop body is empty, so only the deadline check runs.
        $this->clock->allows('getUnixTimestamp')->andReturn(1000, 1030);

        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitThrowsWhenAlreadyPastDeadlineOnFirstLoopIteration(): void
    {
        $this->clock->allows('getUnixTimestamp')->andReturn(1000, 9999);

        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $this->channel->wait(5, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitDeliversMsgToConsumerCallbackBeforeDeadline(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            static function (AMQPEnvelope $envelope): void {
                // Assert inside the callback - assertions after wait() are unreachable when
                // the callback throws StopConsumptionException to exit the loop.
                static::assertSame('hello from wait', $envelope->getBody());
                static::assertSame('my-tag', $envelope->getConsumerTag());
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );

        // Clock never reaches the deadline - the callback terminates the loop first.
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [
                $this->buildSqsMessage('hello from wait', 'receipt-wait', ['routing_key' => 'some_key']),
            ],
        ]));

        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitAutoAcksMessageDuringConsumptionWhenFlagTrue(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function (AMQPEnvelope $envelope): void {
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-autoack-wait')],
        ]));

        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-autoack-wait'])
            ->once();
        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitStoresDeliveryTagForManualAckDuringConsumption(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $capturedTag = null;
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            function (AMQPEnvelope $envelope) use (&$capturedTag): void {
                $capturedTag = $envelope->getDeliveryTag();
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );

        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-manual-ack')],
        ]));

        try {
            $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
            // @phpstan-ignore-next-line - StopConsumptionException propagates from the callback inside wait()
        } catch (StopConsumptionException) {
            // Expected - callback exits the loop.
        }

        static::assertNotNull($capturedTag);
        // After wait(), manual ack via the stored delivery tag must succeed.
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-manual-ack'])
            ->once();
        $this->channel->basicAck($capturedTag, false, AMQPQueueException::class, 'AMQPQueue::ack');
    }

    public function testWaitCalculatesPollDurationFromRemainingTime(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function (AMQPEnvelope $envelope): void {
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        // 45 seconds of timeout remaining → poll duration capped at 20.
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('deleteMessage')->andReturn(new Result([]));

        $this->sqsClient->expects('receiveMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(20, $args['WaitTimeSeconds']);

                return new Result([
                    'Messages' => [$this->buildSqsMessage('hello', 'receipt-poll')],
                ]);
            });

        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(45, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitPollDurationIsShortWhenLittleTimeRemains(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function (AMQPEnvelope $envelope): void {
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        // Deadline = 1000 + 3 = 1003; now = 1000; remaining = 3 → poll duration = 3.
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('deleteMessage')->andReturn(new Result([]));

        $this->sqsClient->expects('receiveMessage')
            ->once()
            ->andReturnUsing(function (array $args): Result {
                static::assertSame(3, $args['WaitTimeSeconds']);

                return new Result([
                    'Messages' => [$this->buildSqsMessage('hello', 'receipt-short')],
                ]);
            });
        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(3, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitThrowsOnSqsReceiveFailure(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            static function () {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('receiveMessage')->andThrow(new Exception('SQS down'));

        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('AMQPChannel::consume(): Failed to receive messages: SQS down');

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitSleepsForTheDefaultDurationWhileTheGlobalPrefetchWindowIsFull(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');

        // Channel-global prefetch of 2, then fill that window with two unacknowledged messages
        // fetched via basicGet() (each call polls SQS exactly once).
        $this->channel->basicQos(0, 2, true, AMQPChannelException::class, 'AMQPChannel::qos');
        $this->sqsClient->expects('receiveMessage')
            ->twice()
            ->andReturn(
                new Result(['Messages' => [$this->buildSqsMessage('m1', 'receipt-1')]]),
                new Result(['Messages' => [$this->buildSqsMessage('m2', 'receipt-2')]])
            );
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            static function (): void {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        // Deadline = 1000 + 30 = 1030. First iteration finds the window full and sleeps without
        // polling; the second iteration is past the deadline and bails out. The two receiveMessage
        // calls expected above are therefore the basicGet() ones only - wait() never polls here.
        $this->clock->allows('getUnixTimestamp')->andReturn(1000, 1000, 1030);

        // Default prefetchFullWaitMicroseconds is 10us.
        $this->sleeper->expects()
            ->sleep(10)
            ->once();

        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitSleepsForAConfiguredNonDefaultDurationWhileTheGlobalPrefetchWindowIsFull(): void
    {
        $channel = new Channel(
            $this->channelFactory,
            $this->transport,
            $this->sqsClient,
            $this->registry,
            $this->bindingMatcher,
            $this->deadLetterHandler,
            $this->publisher,
            new EnvelopeTransformer(),
            new QueueInspector($this->sqsClient, $this->logger),
            new QueueUrlResolver($this->registry, $this->sqsClient, $this->logger, $this->sanitiser),
            $this->sanitiser,
            42,
            $this->logger,
            $this->clock,
            $this->sleeper,
            120,
            100,
            250
        );
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $channel->basicQos(0, 1, true, AMQPChannelException::class, 'AMQPChannel::qos');
        $this->sqsClient->allows('receiveMessage')
            ->andReturn(new Result(['Messages' => [$this->buildSqsMessage('m1', 'receipt-1')]]));
        $channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            false,
            false,
            static function (): void {},
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->clock->allows('getUnixTimestamp')->andReturn(1000, 1000, 1030);

        $this->sleeper->expects()
            ->sleep(250)
            ->once();
        $this->expectException(AMQPChannelException::class);
        $this->expectExceptionMessage('Consumer timeout exceed');

        $channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testWaitDoesNotSleepWhileTheGlobalPrefetchWindowStillHasSpace(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->channel->basicQos(0, 5, true, AMQPChannelException::class, 'AMQPChannel::qos');
        $this->channel->basicConsume(
            'my_queue',
            'my-tag',
            false,
            true,
            false,
            static function (AMQPEnvelope $envelope): void {
                throw new StopConsumptionException();
            },
            AMQPQueueException::class,
            'AMQPQueue::consume'
        );
        $this->clock->allows('getUnixTimestamp')->andReturn(1000);
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-space')],
        ]));
        $this->sqsClient->allows('deleteMessage')->andReturn(new Result([]));

        // The window has room, so the busy-wait sleep must never be reached.
        $this->sleeper->expects('sleep')
            ->never();
        $this->expectException(StopConsumptionException::class);

        $this->channel->wait(30, AMQPChannelException::class, 'AMQPChannel::consume');
    }

    public function testDeclareQueueRecordsDeadLetterExchangeWithoutCreatingAQueueOrRedrivePolicy(): void
    {
        $this->sqsClient->expects('createQueue')
            ->with(['QueueName' => '[sanitised]main_queue[/sanitised]'])
            ->once()
            ->andReturn(new Result(['QueueUrl' => 'https://sqs.example.com/main_queue']));
        // fetchApproximateMessageCount logs a warning and returns 0 on failure.
        $this->logger->allows('warning');

        $this->sqsClient->expects('createQueue')
            ->with(Mockery::on(
                static fn (array $a) => isset($a['QueueName']) && $a['QueueName'] !== '[sanitised]main_queue[/sanitised]'
            ))
            ->never();
        $this->sqsClient->expects('setQueueAttributes')
            ->never();
        $this->registry->expects('setQueue')
            ->once()
            ->andReturnUsing(function (
                string $name,
                string $url,
                bool $durable,
                bool $exclusive,
                bool $autoDelete,
                ?string $deadLetterExchange,
                ?string $deadLetterRoutingKey,
                ?int $messageTtlMs
            ): void {
                static::assertSame('main_queue', $name);
                static::assertSame('https://sqs.example.com/main_queue', $url);
                static::assertSame('dlx_exchange', $deadLetterExchange);
                static::assertNull($deadLetterRoutingKey);
                static::assertNull($messageTtlMs);
            });

        $this->channel->declareQueue(
            'main_queue',
            false,
            true,
            false,
            false,
            false,
            ['x-dead-letter-exchange' => 'dlx_exchange'],
            AMQPQueueException::class,
            'AMQPQueue::declare'
        );
    }

    public function testDeclareQueueDoesNotRecordDeadLetterExchangeWhenArgumentAbsent(): void
    {
        $this->logger->allows('warning');

        $this->sqsClient->expects('createQueue')
            ->with(['QueueName' => '[sanitised]plain_queue[/sanitised]'])
            ->once()
            ->andReturn(new Result(['QueueUrl' => 'https://sqs.example.com/plain_queue']));
        $this->sqsClient->expects('createQueue')
            ->never()
            ->with(Mockery::on(
                static fn (array $a) => isset($a['QueueName']) && $a['QueueName'] !== '[sanitised]plain_queue[/sanitised]'
            ));
        $this->sqsClient->expects('setQueueAttributes')
            ->never();
        $this->registry->expects('setQueue')
            ->once()
            ->andReturnUsing(function (
                string $name,
                string $url,
                bool $durable,
                bool $exclusive,
                bool $autoDelete,
                ?string $deadLetterExchange,
                ?string $deadLetterRoutingKey,
                ?int $messageTtlMs
            ): void {
                static::assertNull($deadLetterExchange);
                static::assertNull($deadLetterRoutingKey);
                static::assertNull($messageTtlMs);
            });

        $this->channel->declareQueue(
            'plain_queue',
            false,
            true,
            false,
            false,
            false,
            [],
            AMQPQueueException::class,
            'AMQPQueue::declare'
        );
    }

    public function testDeclareQueueRecordsTheDeadLetterRoutingKeyWhenProvided(): void
    {
        $this->sqsClient->allows('createQueue')->andReturnUsing(static fn (array $args): Result => new Result([
            'QueueUrl' => 'https://sqs.example.com/' . $args['QueueName'],
        ]));
        $this->logger->allows('warning');

        $this->sqsClient->expects('setQueueAttributes')
            ->never();
        $this->registry->expects('setQueue')
            ->once()
            ->andReturnUsing(function (
                string $name,
                string $url,
                bool $durable,
                bool $exclusive,
                bool $autoDelete,
                ?string $deadLetterExchange,
                ?string $deadLetterRoutingKey,
                ?int $messageTtlMs
            ): void {
                static::assertSame('my_dlx', $deadLetterExchange);
                static::assertSame('failed', $deadLetterRoutingKey);
                static::assertNull($messageTtlMs);
            });

        $this->channel->declareQueue(
            'main_queue',
            false,
            true,
            false,
            false,
            false,
            ['x-dead-letter-exchange' => 'my_dlx', 'x-dead-letter-routing-key' => 'failed'],
            AMQPQueueException::class,
            'AMQPQueue::declare'
        );
    }

    public function testBasicNackWithoutRequeueHandsTheMessageToTheDeadLetterHandlerBeforeDeletingIt(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('dead letter body', 'receipt-dlq', ['routing_key' => 'rk'])],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->deadLetterHandler->expects('deadLetter')
            ->with(Mockery::type(EnvelopeInterface::class))
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-dlq'])
            ->once();

        $this->channel->basicNack(1, false, false, AMQPQueueException::class, 'AMQPQueue::nack');
    }

    public function testBasicNackWithMultipleAndWithoutRequeueHandsEachMessageToTheDeadLetterHandler(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(
            new Result(['Messages' => [$this->buildSqsMessage('msg1', 'receipt-m1')]]),
            new Result(['Messages' => [$this->buildSqsMessage('msg2', 'receipt-m2')]]),
            new Result(['Messages' => [$this->buildSqsMessage('msg3', 'receipt-m3')]])
        );
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        // Tags 1 and 2 are ≤ 2, so both are dead-lettered; tag 3 is excluded.
        $this->deadLetterHandler->expects('deadLetter')
            ->twice();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-m1'])
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-m2'])
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-m3'])
            ->never();

        $this->channel->basicNack(2, true, false, AMQPQueueException::class, 'AMQPQueue::nack');
    }

    public function testBasicRejectWithoutRequeueHandsTheMessageToTheDeadLetterHandlerBeforeDeletingIt(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('reject body', 'receipt-rej-dlq')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->deadLetterHandler->expects('deadLetter')
            ->with(Mockery::type(EnvelopeInterface::class))
            ->once();
        $this->sqsClient->expects('deleteMessage')
            ->with(['QueueUrl' => 'https://sqs.example.com/my_queue', 'ReceiptHandle' => 'receipt-rej-dlq'])
            ->once();

        $this->channel->basicReject(1, false, AMQPQueueException::class, 'AMQPQueue::reject');
    }

    public function testDeclareQueueStoresTtlInRegistryWhenXMessageTtlSet(): void
    {
        $this->logger->allows('warning');

        $this->sqsClient->expects('createQueue')
            ->with(['QueueName' => '[sanitised]retry_queue[/sanitised]'])
            ->once()
            ->andReturn(new Result(['QueueUrl' => 'https://sqs.example.com/retry_queue']));
        $this->sqsClient->expects('setQueueAttributes')
            ->never();
        $this->registry->expects('setQueue')
            ->once()
            ->andReturnUsing(function (
                string $name,
                string $url,
                bool $durable,
                bool $exclusive,
                bool $autoDelete,
                ?string $deadLetterExchange,
                ?string $deadLetterRoutingKey,
                ?int $messageTtlMs
            ): void {
                static::assertSame('retry_queue', $name);
                static::assertNull($deadLetterExchange);
                static::assertNull($deadLetterRoutingKey);
                static::assertSame(60000, $messageTtlMs);
            });

        $this->channel->declareQueue(
            'retry_queue',
            false,
            true,
            false,
            false,
            false,
            ['x-message-ttl' => 60000],
            AMQPQueueException::class,
            'AMQPQueue::declare'
        );
    }

    public function testDeclareQueueDoesNotStoreTtlWhenArgumentAbsent(): void
    {
        $this->sqsClient->allows('createQueue')
            ->andReturn(new Result(['QueueUrl' => 'https://sqs.example.com/q']));
        $this->logger->allows('warning');

        $this->registry->expects('setQueue')
            ->once()
            ->andReturnUsing(function (
                string $name,
                string $url,
                bool $durable,
                bool $exclusive,
                bool $autoDelete,
                ?string $deadLetterExchange,
                ?string $deadLetterRoutingKey,
                ?int $messageTtlMs
            ): void {
                static::assertNull($messageTtlMs);
            });

        $this->channel->declareQueue(
            'plain_queue',
            false,
            true,
            false,
            false,
            false,
            [],
            AMQPQueueException::class,
            'AMQPQueue::declare'
        );
    }

    public function testBasicNackWithRequeueDoesNotDeadLetterTheMessage(): void
    {
        $this->registry->allows()
            ->getQueueUrl('my_queue')
            ->andReturn('https://sqs.example.com/my_queue');
        $this->sqsClient->allows('receiveMessage')->andReturn(new Result([
            'Messages' => [$this->buildSqsMessage('hello', 'receipt-requeue')],
        ]));
        $this->channel->basicGet('my_queue', false, AMQPQueueException::class, 'AMQPQueue::get');

        $this->deadLetterHandler->expects('deadLetter')
            ->never();
        $this->sqsClient->expects('changeMessageVisibility')
            ->with([
                'QueueUrl' => 'https://sqs.example.com/my_queue',
                'ReceiptHandle' => 'receipt-requeue',
                'VisibilityTimeout' => 0,
            ])
            ->once();

        $this->channel->basicNack(1, false, true, AMQPQueueException::class, 'AMQPQueue::nack');
    }

    /**
     * @return array<string, array{bool}>
     */
    public static function booleanDataProvider(): array
    {
        return [
            'true' => [true],
            'false' => [false],
        ];
    }

    /**
     * Builds a minimal SQS message array as returned by receiveMessage().
     *
     * @param array<string, mixed> $metadata
     * @return array<string, mixed>
     */
    private function buildSqsMessage(
        string $body,
        string $receiptHandle,
        array $metadata = [],
        int $receiveCount = 1
    ): array {
        return [
            'Body' => $body,
            'ReceiptHandle' => $receiptHandle,
            'Attributes' => ['ApproximateReceiveCount' => (string) $receiveCount],
            'MessageAttributes' => [
                'amqp_metadata' => [
                    'StringValue' => json_encode($metadata, JSON_THROW_ON_ERROR),
                    'DataType' => 'String',
                ],
            ],
        ];
    }
}
