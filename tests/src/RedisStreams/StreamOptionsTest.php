<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\RedisStreams;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use Waffle\Commons\Queue\RedisStreams\StreamOptions;
use WaffleTests\Commons\Queue\AbstractTestCase as TestCase;

#[CoversClass(StreamOptions::class)]
final class StreamOptionsTest extends TestCase
{
    public function testNamesTheKeysOfAQueue(): void
    {
        $options = new StreamOptions('host-42');

        static::assertSame('host-42', $options->consumer);
        static::assertSame('waffle', $options->group);
        static::assertSame(60_000, $options->claimIdleMs);
        static::assertSame(100, $options->promoteBatch);
        static::assertSame('waffle:queue:mail', $options->stream('mail'));
        static::assertSame('waffle:queue:mail:delayed', $options->delayed('mail'));
        static::assertSame('waffle:queue:mail:unreadable', $options->unreadable('mail'));
    }

    /**
     * @return iterable<string, array{array<string, string|int>, string}>
     */
    public static function invalidOptions(): iterable
    {
        yield 'no consumer' => [['consumer' => ''], 'The stream option "consumer" must not be empty.'];
        yield 'no prefix' => [['prefix' => ''], 'The stream option "prefix" must not be empty.'];
        yield 'no group' => [['group' => ''], 'The stream option "group" must not be empty.'];
        yield 'no claim idle time' => [['claimIdleMs' => 0], 'The stream option "claimIdleMs" must be at least 1.'];
        yield 'no promotion batch' => [['promoteBatch' => 0], 'The stream option "promoteBatch" must be at least 1.'];
    }

    /**
     * @param array<string, string|int> $override
     */
    #[DataProvider('invalidOptions')]
    public function testRefusesAnInvalidOption(array $override, string $reason): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($reason);

        new \ReflectionClass(StreamOptions::class)->newInstanceArgs(['consumer' => 'host-42', ...$override]);
    }
}
