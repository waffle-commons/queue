<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\RedisStreams;

use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use Redis;
use stdClass;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Telemetry\SpanContextInterface;
use Waffle\Commons\Contracts\Telemetry\TextMapPropagatorInterface;
use Waffle\Commons\Contracts\Telemetry\TracerInterface;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use Waffle\Commons\Queue\Exception\QueueException;
use Waffle\Commons\Queue\RedisStreams\RedisStreamsQueue;
use WaffleTests\Commons\Queue\Fixture\MovableClock;
use WaffleTests\Commons\Queue\Fixture\RedisPool;
use WaffleTests\Commons\Queue\Fixture\SendInvoice;
use WaffleTests\Commons\Queue\Fixture\UnhealthyPoolException;
use WaffleTests\Commons\Queue\RedisTestCase;

#[CoversClass(RedisStreamsQueue::class)]
#[CoversClass(QueueException::class)]
#[CoversClass(EnvelopeCodec::class)]
final class RedisStreamsQueueTest extends RedisTestCase
{
    public function testADispatchedMessageIsDeliveredOnceThenAcknowledged(): void
    {
        $pool = self::pool();
        $queue = new RedisStreamsQueue($pool, self::codec(), new MovableClock(), $this->options());

        $dispatched = $queue->dispatch(new SendInvoice('inv-1'), 'invoices');
        $received = $queue->receive('invoices');

        static::assertNotNull($received);
        static::assertSame($dispatched->id, $received->id);
        static::assertEquals(new SendInvoice('inv-1'), $received->message);
        static::assertSame(0, $received->attempts);
        static::assertNotNull($received->receipt);
        static::assertNull($queue->receive('invoices'), 'A delivery in flight is not handed out twice.');

        $queue->acknowledge($received);

        static::assertNull($queue->receive('invoices'));
        static::assertSame(0, self::connect()->xLen($this->prefix . 'invoices'), 'An acknowledged entry is deleted.');
        static::assertSame($pool->acquired, $pool->released, 'Every lease is released.');
    }

    public function testMessagesDispatchedBeforeAnyWorkerAreDeliveredInOrder(): void
    {
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());
        $first = $queue->dispatch(new SendInvoice('inv-1'));
        $second = $queue->dispatch(new SendInvoice('inv-2'));

        static::assertSame($first->id, $queue->receive('default')?->id);
        static::assertSame($second->id, $queue->receive('default', 50)?->id);
    }

    public function testADelayedMessageWaitsUntilItIsDue(): void
    {
        $clock = new MovableClock();
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), $clock, $this->options());
        $queue->dispatch(new SendInvoice('inv-1'), availableAt: $clock->now()->modify('+5 minutes'));

        static::assertNull($queue->receive('default'));

        $clock->advance('+5 minutes');
        $received = $queue->receive('default');
        static::assertNotNull($received);
        static::assertEquals(new SendInvoice('inv-1'), $received->message);
    }

    public function testAReleasedMessageComesBackLaterAsANewAttempt(): void
    {
        $clock = new MovableClock();
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), $clock, $this->options());
        $queue->dispatch(new SendInvoice('inv-1'));
        $received = $queue->receive('default');
        static::assertNotNull($received);

        $queue->release($received, $clock->now()->modify('+30 seconds'));

        static::assertNull($queue->receive('default'));
        $clock->advance('+30 seconds');
        $again = $queue->receive('default');
        static::assertNotNull($again);
        static::assertSame($received->id, $again->id);
        static::assertSame(1, $again->attempts);
    }

    public function testAMessageReleasedForNowIsAvailableAtOnce(): void
    {
        $clock = new MovableClock();
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), $clock, $this->options());
        $queue->dispatch(new SendInvoice('inv-1'));
        $received = $queue->receive('default');
        static::assertNotNull($received);

        $queue->release($received, $clock->now());

        static::assertSame(1, $queue->receive('default')?->attempts);
    }

    /**
     * [QUEUE-02] acceptance: a message survives a worker crash — the delivery a
     * stopped worker left pending is reclaimed by another and processed again.
     */
    public function testAMessageSurvivesAWorkerCrash(): void
    {
        $clock = new MovableClock();
        $crashed = new RedisStreamsQueue(self::pool(), self::codec(), $clock, $this->options('worker-1', 1));
        $survivor = new RedisStreamsQueue(self::pool(), self::codec(), $clock, $this->options('worker-2', 1));
        $dispatched = $crashed->dispatch(new SendInvoice('inv-1'));
        static::assertNotNull($crashed->receive('default'));

        // worker-1 stops without settling the delivery; once it has been idle
        // longer than the claim time, worker-2 takes it over.
        usleep(5_000);
        $reclaimed = $survivor->receive('default');

        static::assertNotNull($reclaimed);
        static::assertSame($dispatched->id, $reclaimed->id);
        static::assertSame(1, $reclaimed->attempts, 'The crash counts as a failed attempt.');

        $survivor->acknowledge($reclaimed);
        usleep(5_000);
        static::assertNull($survivor->receive('default'));
    }

    public function testAnEntryNoHandlerAllowsIsSetAsideInsteadOfBlockingTheQueue(): void
    {
        $redis = self::connect();
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());
        $redis->xAdd($this->prefix . 'default', '*', ['envelope' => '{"type":"App\\\\Evil"}']);
        $queue->dispatch(new SendInvoice('inv-1'));

        static::assertNull($queue->receive('default'));
        static::assertEquals(new SendInvoice('inv-1'), $queue->receive('default')?->message);
        static::assertSame(1, $redis->xLen($this->prefix . 'default:unreadable'));
    }

    public function testAnEntryWithoutAnEnvelopeFieldIsIgnored(): void
    {
        $redis = self::connect();
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());
        $redis->xAdd($this->prefix . 'default', '*', ['other' => 'field']);

        static::assertNull($queue->receive('default'));
    }

    public function testTheDispatcherRecordsTheCurrentTraceContext(): void
    {
        $context = $this->createStub(SpanContextInterface::class);
        $tracer = $this->createStub(TracerInterface::class);
        $tracer->method('currentContext')->willReturn($context);
        $propagator = new class implements TextMapPropagatorInterface {
            #[\Override]
            public function inject(SpanContextInterface $context, array &$carrier): void
            {
                $carrier['traceparent'] = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
            }

            #[\Override]
            public function extract(array $carrier): ?SpanContextInterface
            {
                return null;
            }
        };
        $queue = new RedisStreamsQueue(
            self::pool(),
            self::codec(),
            new MovableClock(),
            $this->options(),
            $tracer,
            $propagator,
        );

        $queue->dispatch(new SendInvoice('inv-1'));

        static::assertSame(
            ['traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'],
            $queue->receive('default')?->traceContext,
        );
    }

    public function testSettlingAnEnvelopeThatWasNeverReceivedFails(): void
    {
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());
        $envelope = new Envelope('msg-1', new SendInvoice('inv-1'), 'default', new DateTimeImmutable());

        try {
            $queue->acknowledge($envelope);
            static::fail('An envelope without a receipt cannot be acknowledged.');
        } catch (QueueException $exception) {
            static::assertStringContainsString('has no receipt', $exception->getMessage());
        }

        $this->expectException(QueueException::class);
        $queue->release($envelope, new DateTimeImmutable());
    }

    public function testAMessageTheSerializerRefusesIsNotDispatched(): void
    {
        $queue = new RedisStreamsQueue(
            self::pool(),
            new EnvelopeCodec(new \WaffleTests\Commons\Queue\Fixture\InvoiceSerializer(), []),
            new MovableClock(),
            $this->options(),
        );
        $message = new class implements \Waffle\Commons\Contracts\Queue\MessageInterface {};

        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('cannot be encoded');

        $queue->dispatch($message);
    }

    public function testAPoolThatHandsOutAnotherClientIsRefused(): void
    {
        $pool = new RedisPool(static fn(): object => new stdClass());
        $queue = new RedisStreamsQueue($pool, self::codec(), new MovableClock(), $this->options());

        try {
            $queue->receive('default');
            static::fail('Only an ext-redis client is accepted.');
        } catch (QueueException $exception) {
            static::assertSame(
                'The Redis pool hands out a "stdClass", not an ext-redis client.',
                $exception->getMessage(),
            );
        }

        static::assertSame(1, $pool->released, 'The lease is released all the same.');
    }

    public function testAPoolWithoutAHealthyConnectionIsATransportFailure(): void
    {
        $pool = new RedisPool(static function (): never {
            throw new UnhealthyPoolException();
        });
        $queue = new RedisStreamsQueue($pool, self::codec(), new MovableClock(), $this->options());

        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('The queue transport failed: No healthy client.');

        $queue->dispatch(new SendInvoice('inv-1'));
    }

    public function testARedisThatIsUnreachableIsATransportFailure(): void
    {
        $unreachable = new Redis();
        $queue = new RedisStreamsQueue(
            new RedisPool(static fn(): Redis => $unreachable),
            self::codec(),
            new MovableClock(),
            $this->options(),
        );
        $envelope = new Envelope('msg-1', new SendInvoice('inv-1'), 'default', new DateTimeImmutable(), receipt: '0-1');

        foreach ([
            static fn() => $queue->dispatch(new SendInvoice('inv-1')),
            static fn() => $queue->receive('default'),
            static fn() => $queue->acknowledge($envelope),
            static fn() => $queue->release($envelope, new DateTimeImmutable()),
        ] as $operation) {
            try {
                $operation();
                static::fail('An unreachable Redis must fail.');
            } catch (QueueException $exception) {
                static::assertStringStartsWith('The queue transport failed: ', $exception->getMessage());
            }
        }
    }

    public function testACommandRedisRefusesIsAQueueFailure(): void
    {
        // phpredis reports a server error as a `false` reply, not an exception:
        // a key of the wrong type makes every command refuse.
        $redis = self::connect();
        $redis->set($this->prefix . 'default', 'not a stream');
        $redis->set($this->prefix . 'default:delayed', 'not a sorted set');
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());
        $envelope = new Envelope('msg-1', new SendInvoice('inv-1'), 'default', new DateTimeImmutable(), receipt: '0-1');

        foreach ([
            static fn() => $queue->dispatch(new SendInvoice('inv-1')),
            static fn() => $queue->dispatch(new SendInvoice('inv-1'), availableAt: new DateTimeImmutable('+1 hour')),
            static fn() => $queue->receive('default'),
            static fn() => $queue->acknowledge($envelope),
            static fn() => $queue->release($envelope, new DateTimeImmutable()),
        ] as $operation) {
            try {
                $operation();
                static::fail('A refused command must fail.');
            } catch (QueueException $exception) {
                static::assertStringStartsWith('Redis refused the command: WRONGTYPE', $exception->getMessage());
            }
        }
    }

    public function testAClaimRedisRefusesIsAQueueFailure(): void
    {
        $redis = self::connect();
        $redis->set($this->prefix . 'other', 'not a stream');
        $queue = new RedisStreamsQueue(self::pool(), self::codec(), new MovableClock(), $this->options());

        $this->expectException(QueueException::class);
        $this->expectExceptionMessage('Redis refused the command: WRONGTYPE');

        $queue->receive('other');
    }
}
