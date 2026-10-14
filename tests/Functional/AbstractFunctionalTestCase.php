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

namespace Envoylope\Amqp\Driver\Sqs\Tests\Functional;

use Asmblah\PhpAmqpCompat\Configuration\Configuration;
use Aws\Sqs\SqsClient;
use Envoylope\Amqp\Driver\Sqs\Setup\ExistingSetup;
use Envoylope\Amqp\Driver\Sqs\SqsDriver;
use Envoylope\Amqp\Driver\Sqs\Tests\AbstractTestCase;
use Envoylope\Amqp\Driver\Sqs\Topology\CacheBackedTopologyStorage;
use Psr\Cache\CacheItemPoolInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

/**
 * Class AbstractFunctionalTestCase.
 *
 * Base class for all functional test cases.
 * Provides a shared SqsClient (pointing at ElasticMQ) and a per-test in-memory cache.
 *
 * ElasticMQ must be running on localhost:9324 before executing functional tests locally:
 *   docker-compose up -d
 *
 * @author Dan Phillimore <dan@ovms.co>
 */
abstract class AbstractFunctionalTestCase extends AbstractTestCase
{
    protected CacheItemPoolInterface $cache;
    protected SqsClient $sqsClient;

    public function setUp(): void
    {
        $this->sqsClient = new SqsClient([
            'version' => 'latest',
            'region' => 'us-east-1',
            'endpoint' => 'http://localhost:9324',
            'credentials' => [
                'key' => 'x',
                'secret' => 'x',
            ],
        ]);

        // A fresh in-memory cache per test prevents binding state from leaking between tests.
        $this->cache = new ArrayAdapter();
    }

    /**
     * Creates a Configuration wired to the SQS driver for this test.
     */
    protected function createConfiguration(): Configuration
    {
        return new Configuration(
            logger: new NullLogger(),
            driver: new SqsDriver(
                new ExistingSetup(
                    $this->sqsClient,
                    new CacheBackedTopologyStorage($this->cache)
                )
            )
        );
    }
}
