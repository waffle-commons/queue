<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue;

use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Redis;
use RedisException;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use Waffle\Commons\Queue\RedisStreams\StreamOptions;
use WaffleTests\Commons\Queue\AbstractTestCase as TestCase;
use WaffleTests\Commons\Queue\Fixture\InvoiceSerializer;
use WaffleTests\Commons\Queue\Fixture\RedisPool;
use WaffleTests\Commons\Queue\Fixture\SendInvoice;

/**
 * Integration tests against a real Redis (`WAFFLE_TEST_REDIS_HOST`, the dev
 * stack's `waffle-redis` by default). Each test works under its own key prefix
 * and removes its keys; without a reachable Redis (a bare CI runner) the tests
 * are skipped, like data's Redis integration test.
 */
#[RequiresPhpExtension('redis')]
abstract class RedisTestCase extends TestCase
{
    protected string $prefix = '';

    #[\Override]
    protected function setUp(): void
    {
        parent::setUp();
        $this->prefix = 'waffle:test:' . bin2hex(random_bytes(6)) . ':';
    }

    #[\Override]
    protected function tearDown(): void
    {
        $redis = self::connect();
        $keys = $redis->keys($this->prefix . '*');
        if (is_array($keys) && $keys !== []) {
            $redis->del($keys);
        }

        parent::tearDown();
    }

    protected static function connect(): Redis
    {
        $host = getenv('WAFFLE_TEST_REDIS_HOST');
        $redis = new Redis();

        try {
            $redis->connect(is_string($host) && $host !== '' ? $host : 'waffle-redis', 6379, 1.5);
        } catch (RedisException) {
            self::markTestSkipped('No reachable Redis instance for the integration test.');
        }

        return $redis;
    }

    /**
     * A client that can never reach a server: its connection to port 1 was
     * refused. A never-connected `new Redis()` is not enough, because phpredis
     * then talks to 127.0.0.1:6379, where CI runs a real Redis.
     */
    protected static function unreachableRedis(): Redis
    {
        $redis = new Redis();

        try {
            $connected = $redis->connect('127.0.0.1', 1, 0.5);
        } catch (RedisException) {
            return $redis;
        }

        if ($connected) {
            static::fail('Something listens on 127.0.0.1:1; the unreachable Redis needs a closed port.');
        }

        return $redis;
    }

    protected static function pool(): RedisPool
    {
        $redis = self::connect();

        return new RedisPool(static fn(): Redis => $redis);
    }

    protected static function codec(): EnvelopeCodec
    {
        return new EnvelopeCodec(new InvoiceSerializer(), [SendInvoice::class]);
    }

    protected function options(string $consumer = 'worker-1', int $claimIdleMs = 60_000): StreamOptions
    {
        return new StreamOptions($consumer, $this->prefix, claimIdleMs: $claimIdleMs);
    }
}
