<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Health;

use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use stdClass;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionPoolInterface;
use Waffle\Commons\Contracts\Health\Enum\HealthStatus;
use Waffle\Commons\Queue\Health\StreamsHealthCheck;
use Waffle\Commons\Queue\RedisStreams\StreamOptions;
use WaffleTests\Commons\Queue\Fixture\RedisPool;
use WaffleTests\Commons\Queue\RedisTestCase;

#[CoversClass(StreamsHealthCheck::class)]
final class StreamsHealthCheckTest extends RedisTestCase
{
    public function testUpWithTheNumberOfWaitingMessages(): void
    {
        $options = new StreamOptions('worker-1', $this->prefix);
        self::connect()->xAdd($options->stream('invoices'), '*', ['payload' => '{}']);

        $result = new StreamsHealthCheck(self::pool(), $options, 'invoices')->check();

        static::assertSame(HealthStatus::Up, $result->status);
        static::assertSame('1 message(s) waiting', $result->detail);
    }

    public function testUpOnAQueueThatNeverReceivedAMessage(): void
    {
        $result = new StreamsHealthCheck(self::pool(), new StreamOptions('worker-1', $this->prefix))->check();

        static::assertSame(HealthStatus::Up, $result->status);
        static::assertSame('0 message(s) waiting', $result->detail);
    }

    public function testDownWhenThePoolCannotLease(): void
    {
        $pool = $this->createStub(RedisConnectionPoolInterface::class);
        $pool->method('acquire')->willThrowException(new RuntimeException('Connection refused'));

        $result = new StreamsHealthCheck($pool, new StreamOptions('worker-1', $this->prefix))->check();

        static::assertSame(HealthStatus::Down, $result->status);
        static::assertStringContainsString('Connection refused', $result->detail);
    }

    public function testDownWhenThePoolHandsOutAnotherClient(): void
    {
        $pool = new RedisPool(static fn(): object => new stdClass());

        $result = new StreamsHealthCheck($pool, new StreamOptions('worker-1', $this->prefix))->check();

        static::assertSame(HealthStatus::Down, $result->status);
        static::assertStringContainsString('ext-redis', $result->detail);
    }

    public function testNamedQueueAndCriticalByDefault(): void
    {
        $check = new StreamsHealthCheck(self::pool(), new StreamOptions('worker-1', $this->prefix));

        static::assertSame('queue', $check->name());
        static::assertTrue($check->isCritical());
    }
}
