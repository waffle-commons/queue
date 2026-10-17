<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\RedisStreams;

use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use Redis;
use stdClass;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\MessageInterface;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use Waffle\Commons\Queue\Exception\QueueException;
use Waffle\Commons\Queue\RedisStreams\RedisFailedMessageStore;
use WaffleTests\Commons\Queue\Fixture\InvoiceSerializer;
use WaffleTests\Commons\Queue\Fixture\MovableClock;
use WaffleTests\Commons\Queue\Fixture\RedisPool;
use WaffleTests\Commons\Queue\Fixture\SendInvoice;
use WaffleTests\Commons\Queue\Fixture\UnhealthyPoolException;
use WaffleTests\Commons\Queue\RedisTestCase;

#[CoversClass(RedisFailedMessageStore::class)]
#[CoversClass(QueueException::class)]
final class RedisFailedMessageStoreTest extends RedisTestCase
{
    /**
     * [QUEUE-02] acceptance: a failed message lands in the dead-letter store with
     * its full envelope — id, attempts, trace context — and the reason.
     */
    public function testKeepsTheFullEnvelopeTheReasonAndTheInstant(): void
    {
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), new MovableClock(), $this->prefix);
        $envelope = self::envelope('msg-1', 3);

        $store->store($envelope, 'RuntimeException: the PDF renderer timed out');

        $failed = $store->list();
        static::assertCount(1, $failed);
        $first = $failed[0] ?? null;
        static::assertNotNull($first);
        static::assertSame('msg-1', $first->envelope->id);
        static::assertSame(3, $first->envelope->attempts);
        static::assertSame($envelope->traceContext, $first->envelope->traceContext);
        static::assertEquals(new SendInvoice('inv-1'), $first->envelope->message);
        static::assertSame('RuntimeException: the PDF renderer timed out', $first->reason);
        static::assertSame('2026-10-10T09:00:00+00:00', $first->failedAt->format(DATE_ATOM));
    }

    public function testListsOldestFirstWithinTheLimit(): void
    {
        $clock = new MovableClock();
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), $clock, $this->prefix);
        $store->store(self::envelope('msg-1'), 'first');
        $clock->advance('+1 second');
        $store->store(self::envelope('msg-2'), 'second');
        $clock->advance('+1 second');
        $store->store(self::envelope('msg-1'), 'failed again');

        static::assertSame(
            ['second', 'failed again'],
            array_map(static fn($failed): string => $failed->reason, $store->list()),
        );
        static::assertCount(1, $store->list(1));
        static::assertSame([], $store->list(0));
    }

    public function testForgetsARemovedMessage(): void
    {
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), new MovableClock(), $this->prefix);
        $store->store(self::envelope('msg-1'), 'first');

        $store->remove('msg-1');

        static::assertSame([], $store->list());
    }

    public function testSkipsARecordItCanNoLongerRead(): void
    {
        $redis = self::connect();
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), new MovableClock(), $this->prefix);
        $store->store(self::envelope('msg-1'), 'readable');
        $redis->hSet($this->prefix . 'failed', 'msg-2', '{"envelope":"{}","reason":"gone","failed_at":1}');
        $redis->zAdd($this->prefix . 'failed:order', 2, 'msg-2');
        $redis->hSet($this->prefix . 'failed', 'msg-3', 'not json');
        $redis->zAdd($this->prefix . 'failed:order', 3, 'msg-3');
        $redis->zAdd($this->prefix . 'failed:order', 4, 'msg-4');
        $redis->hSet($this->prefix . 'failed', 'msg-5', '{"envelope":"{}","failed_at":5}');
        $redis->zAdd($this->prefix . 'failed:order', 5, 'msg-5');

        static::assertSame(['readable'], array_map(static fn($failed): string => $failed->reason, $store->list()));
    }

    public function testAMessageThatCannotBeEncodedIsNotStored(): void
    {
        $store = new RedisFailedMessageStore(
            self::pool(),
            new EnvelopeCodec(new InvoiceSerializer(), []),
            new MovableClock(),
            $this->prefix,
        );
        $message = new class implements MessageInterface {};

        $this->expectException(QueueException::class);

        $store->store(new Envelope('msg-1', $message, 'default', new DateTimeImmutable()), 'reason');
    }

    public function testTransportFailuresAreQueueExceptions(): void
    {
        $unreachable = new Redis();
        $closed = new RedisFailedMessageStore(
            new RedisPool(static fn(): Redis => $unreachable),
            self::codec(),
            new MovableClock(),
            $this->prefix,
        );
        $foreign = new RedisFailedMessageStore(
            new RedisPool(static fn(): object => new stdClass()),
            self::codec(),
            new MovableClock(),
            $this->prefix,
        );
        $unhealthy = new RedisFailedMessageStore(
            new RedisPool(static function (): never {
                throw new UnhealthyPoolException();
            }),
            self::codec(),
            new MovableClock(),
            $this->prefix,
        );

        foreach ([
            static fn() => $closed->store(self::envelope('msg-1'), 'reason'),
            $closed->list(...),
            static fn() => $closed->remove('msg-1'),
            $foreign->list(...),
            static fn() => $unhealthy->remove('msg-1'),
        ] as $operation) {
            try {
                $operation();
                static::fail('The operation must fail.');
            } catch (QueueException) {
                static::addToAssertionCount(1);
            }
        }
    }

    public function testCommandsRedisRefusesAreQueueExceptions(): void
    {
        $redis = self::connect();
        $redis->set($this->prefix . 'failed', 'not a hash');
        $redis->set($this->prefix . 'failed:order', 'not a sorted set');
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), new MovableClock(), $this->prefix);

        foreach ([
            static fn() => $store->store(self::envelope('msg-1'), 'reason'),
            $store->list(...),
            static fn() => $store->remove('msg-1'),
        ] as $operation) {
            try {
                $operation();
                static::fail('A refused command must fail.');
            } catch (QueueException $exception) {
                static::assertStringStartsWith('Redis refused the command: WRONGTYPE', $exception->getMessage());
            }
        }
    }

    public function testRecordsRedisRefusesToReadAreAQueueFailure(): void
    {
        $redis = self::connect();
        $redis->zAdd($this->prefix . 'failed:order', 1, 'msg-1');
        $redis->set($this->prefix . 'failed', 'not a hash');
        $store = new RedisFailedMessageStore(self::pool(), self::codec(), new MovableClock(), $this->prefix);

        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('Redis refused the command: WRONGTYPE');

        $store->list();
    }

    private static function envelope(string $id, int $attempts = 0): Envelope
    {
        return new Envelope(
            $id,
            new SendInvoice('inv-1'),
            'default',
            new DateTimeImmutable('2026-10-10T08:00:00+00:00'),
            $attempts,
            ['traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'],
        );
    }
}
