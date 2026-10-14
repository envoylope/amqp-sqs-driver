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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Unit\Registry;

use Envoylope\Amqp\Driver\Sqs\Registry\ExchangeRegistry;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Topology\CacheBackedTopologyStorage;
use Envoylope\Amqp\Driver\Sqs\Topology\TopologyStorageInterface;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

/**
 * Class ExchangeRegistryTest.
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
class ExchangeRegistryTest extends AbstractTestCase
{
    private ArrayAdapter $cache;
    private ExchangeRegistry $registry;

    public function setUp(): void
    {
        $this->cache = new ArrayAdapter();

        $this->registry = new ExchangeRegistry(new CacheBackedTopologyStorage($this->cache));
    }

    public function testGetExchangeReturnsNullWhenTopologyIsEmpty(): void
    {
        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testGetExchangeReturnsExchangeWhenSet(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, ['x-arg' => 'val']);

        $exchange = $this->registry->getExchange('my-exchange');

        static::assertNotNull($exchange);
        static::assertSame(AMQP_EX_TYPE_DIRECT, $exchange->getType());
        static::assertTrue($exchange->isDurable());
        static::assertFalse($exchange->isAutoDelete());
        static::assertFalse($exchange->isInternal());
        static::assertSame(['x-arg' => 'val'], $exchange->getArguments());
        static::assertSame([], $exchange->getQueueBindings());
        static::assertSame([], $exchange->getExchangeBindings());
    }

    public function testGetQueueUrlReturnsNullWhenQueueIsUnknown(): void
    {
        static::assertNull($this->registry->getQueueUrl('nonexistent'));
    }

    public function testGetQueueUrlReturnsUrlAfterQueueIsSet(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        static::assertSame('https://sqs.example.com/my-queue', $this->registry->getQueueUrl('my-queue'));
    }

    public function testSetExchangePreservesExistingBindingsOnRedeclaration(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);
        $this->registry->addQueueBinding('my-exchange', 'my-queue', 'my-key');
        $this->registry->addExchangeBinding('my-exchange', 'dest-exchange', 'other-key');

        // Re-declare with different type - bindings must survive.
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_FANOUT, false, true, false, []);

        $exchange = $this->registry->getExchange('my-exchange');
        static::assertNotNull($exchange);
        static::assertSame(AMQP_EX_TYPE_FANOUT, $exchange->getType());
        static::assertCount(1, $exchange->getQueueBindings());
        static::assertCount(1, $exchange->getExchangeBindings());
    }

    public function testSetExchangePersistsInCache(): void
    {
        $this->registry->setExchange('persisted-exchange', AMQP_EX_TYPE_TOPIC, true, false, false, []);

        $topology = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get();
        static::assertSame(AMQP_EX_TYPE_TOPIC, $topology['exchanges']['persisted-exchange']['type']);
    }

    public function testAddQueueBindingAddsBindingToExchange(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);

        $this->registry->addQueueBinding('my-exchange', 'my-queue', 'my-key');

        $exchange = $this->registry->getExchange('my-exchange');
        static::assertNotNull($exchange);
        static::assertCount(1, $exchange->getQueueBindings());
        static::assertSame('my-queue', $exchange->getQueueBindings()[0]->getQueueName());
        static::assertSame('my-key', $exchange->getQueueBindings()[0]->getRoutingKey());
    }

    public function testAddQueueBindingIsIdempotent(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);

        $this->registry->addQueueBinding('my-exchange', 'my-queue', 'my-key');
        $this->registry->addQueueBinding('my-exchange', 'my-queue', 'my-key');

        $exchange = $this->registry->getExchange('my-exchange');
        static::assertNotNull($exchange);
        static::assertCount(1, $exchange->getQueueBindings());
    }

    public function testAddQueueBindingDoesNothingWhenExchangeNotFound(): void
    {
        $this->registry->addQueueBinding('nonexistent', 'my-queue', 'my-key');

        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testRemoveQueueBindingRemovesBinding(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);
        $this->registry->addQueueBinding('my-exchange', 'my-queue', 'my-key');

        $this->registry->removeQueueBinding('my-exchange', 'my-queue', 'my-key');

        $exchange = $this->registry->getExchange('my-exchange');
        static::assertNotNull($exchange);
        static::assertCount(0, $exchange->getQueueBindings());
    }

    public function testRemoveQueueBindingDoesNothingWhenExchangeNotFound(): void
    {
        $this->registry->removeQueueBinding('nonexistent', 'my-queue', 'my-key');

        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testAddExchangeBindingAddsBindingToSourceExchange(): void
    {
        $this->registry->setExchange('src-exchange', AMQP_EX_TYPE_FANOUT, true, false, false, []);

        $this->registry->addExchangeBinding('src-exchange', 'dest-exchange', 'key');

        $exchange = $this->registry->getExchange('src-exchange');
        static::assertNotNull($exchange);
        static::assertCount(1, $exchange->getExchangeBindings());
        static::assertSame('dest-exchange', $exchange->getExchangeBindings()[0]->getDestinationExchangeName());
        static::assertSame('key', $exchange->getExchangeBindings()[0]->getRoutingKey());
    }

    public function testAddExchangeBindingIsIdempotent(): void
    {
        $this->registry->setExchange('src-exchange', AMQP_EX_TYPE_FANOUT, true, false, false, []);

        $this->registry->addExchangeBinding('src-exchange', 'dest-exchange', 'key');
        $this->registry->addExchangeBinding('src-exchange', 'dest-exchange', 'key');

        $exchange = $this->registry->getExchange('src-exchange');
        static::assertNotNull($exchange);
        static::assertCount(1, $exchange->getExchangeBindings());
    }

    public function testAddExchangeBindingDoesNothingWhenExchangeNotFound(): void
    {
        $this->registry->addExchangeBinding('nonexistent', 'dest-exchange', 'key');

        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testRemoveExchangeBindingRemovesBinding(): void
    {
        $this->registry->setExchange('src-exchange', AMQP_EX_TYPE_FANOUT, true, false, false, []);
        $this->registry->addExchangeBinding('src-exchange', 'dest-exchange', 'key');

        $this->registry->removeExchangeBinding('src-exchange', 'dest-exchange', 'key');

        $exchange = $this->registry->getExchange('src-exchange');
        static::assertNotNull($exchange);
        static::assertCount(0, $exchange->getExchangeBindings());
    }

    public function testRemoveExchangeBindingDoesNothingWhenExchangeNotFound(): void
    {
        $this->registry->removeExchangeBinding('nonexistent', 'dest-exchange', 'key');

        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testDeleteExchangeRemovesExchangeFromTopology(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);

        $this->registry->deleteExchange('my-exchange');

        static::assertNull($this->registry->getExchange('my-exchange'));
    }

    public function testDeleteExchangeDoesNothingForUnknownExchange(): void
    {
        $this->registry->deleteExchange('nonexistent');

        static::assertNull($this->registry->getExchange('nonexistent'));
    }

    public function testDeleteExchangePersistsRemovalInCache(): void
    {
        $this->registry->setExchange('my-exchange', AMQP_EX_TYPE_DIRECT, true, false, false, []);

        $this->registry->deleteExchange('my-exchange');

        $topology = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get();
        static::assertArrayNotHasKey('my-exchange', $topology['exchanges']);
    }

    public function testDeleteQueueRemovesQueueFromTopology(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        $this->registry->deleteQueue('my-queue');

        static::assertNull($this->registry->getQueueUrl('my-queue'));
    }

    public function testDeleteQueueDoesNothingForUnknownQueue(): void
    {
        $this->registry->deleteQueue('nonexistent');

        static::assertNull($this->registry->getQueueUrl('nonexistent'));
    }

    public function testGetDeadLetterExchangeReturnsNullWhenQueueHasNoDlx(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        static::assertNull($this->registry->getDeadLetterExchange('my-queue'));
    }

    public function testGetDeadLetterExchangeReturnsNullForUnknownQueue(): void
    {
        static::assertNull($this->registry->getDeadLetterExchange('nonexistent'));
    }

    public function testGetDeadLetterExchangeReturnsExchangeWithNullRoutingKeyWhenOnlyTheExchangeIsSet(): void
    {
        $this->registry->setQueue(
            'my-queue',
            'https://sqs.example.com/my-queue',
            true,
            false,
            false,
            'my.dlx'
        );

        $deadLetterExchange = $this->registry->getDeadLetterExchange('my-queue');

        static::assertNotNull($deadLetterExchange);
        static::assertSame('my.dlx', $deadLetterExchange->getName());
        static::assertNull($deadLetterExchange->getRoutingKey());
    }

    public function testGetDeadLetterExchangeReturnsTheConfiguredRoutingKeyWhenSet(): void
    {
        $this->registry->setQueue(
            'my-queue',
            'https://sqs.example.com/my-queue',
            true,
            false,
            false,
            'my.dlx',
            'failed'
        );

        $deadLetterExchange = $this->registry->getDeadLetterExchange('my-queue');

        static::assertNotNull($deadLetterExchange);
        static::assertSame('my.dlx', $deadLetterExchange->getName());
        static::assertSame('failed', $deadLetterExchange->getRoutingKey());
    }

    public function testSetQueuePersistsDeadLetterExchangeInCache(): void
    {
        $this->registry->setQueue(
            'my-queue',
            'https://sqs.example.com/my-queue',
            true,
            false,
            false,
            'my.dlx',
            'failed'
        );

        $topology = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get();
        static::assertSame('my.dlx', $topology['queues']['my-queue']['deadLetterExchange']);
        static::assertSame('failed', $topology['queues']['my-queue']['deadLetterRoutingKey']);
    }

    public function testRedeclaringQueueWithNullDlxClearsDeadLetterExchange(): void
    {
        $this->registry->setQueue(
            'my-queue',
            'https://sqs.example.com/my-queue',
            true,
            false,
            false,
            'my.dlx',
            'failed'
        );

        // Re-declare without a DLX.
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        static::assertNull($this->registry->getDeadLetterExchange('my-queue'));
    }

    public function testGetQueueTtlMsReturnsNullWhenQueueHasNoTtl(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        static::assertNull($this->registry->getQueueTtlMs('my-queue'));
    }

    public function testGetQueueTtlMsReturnsNullForUnknownQueue(): void
    {
        static::assertNull($this->registry->getQueueTtlMs('nonexistent'));
    }

    public function testGetQueueTtlMsReturnsTtlWhenSet(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false, null, null, 60000);

        static::assertSame(60000, $this->registry->getQueueTtlMs('my-queue'));
    }

    public function testSetQueuePersistsTtlInCache(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false, null, null, 30000);

        $topology = $this->cache->getItem(TopologyStorageInterface::TOPOLOGY_KEY)->get();
        static::assertSame(30000, $topology['queues']['my-queue']['messageTtlMs']);
    }

    public function testRedeclaringQueueWithNullTtlClearsTtl(): void
    {
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false, null, null, 60000);

        // Re-declare without TTL.
        $this->registry->setQueue('my-queue', 'https://sqs.example.com/my-queue', true, false, false);

        static::assertNull($this->registry->getQueueTtlMs('my-queue'));
    }

    public function testSetQueueCanStoreBothDlxAndTtlTogether(): void
    {
        $this->registry->setQueue(
            'my-queue',
            'https://sqs.example.com/my-queue',
            true,
            false,
            false,
            'my.dlx',
            'failed',
            60000
        );

        $deadLetterExchange = $this->registry->getDeadLetterExchange('my-queue');

        static::assertNotNull($deadLetterExchange);
        static::assertSame('my.dlx', $deadLetterExchange->getName());
        static::assertSame('failed', $deadLetterExchange->getRoutingKey());
        static::assertSame(60000, $this->registry->getQueueTtlMs('my-queue'));
    }
}
