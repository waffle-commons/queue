<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\RedisStreams;

use DateTimeImmutable;
use InvalidArgumentException;
use JsonException;
use Psr\Clock\ClockInterface;
use Redis;
use RedisException;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionInterface;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionPoolInterface;
use Waffle\Commons\Contracts\Data\Exception\DatabaseExceptionInterface;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\MessageInterface;
use Waffle\Commons\Contracts\Queue\QueueConsumerInterface;
use Waffle\Commons\Contracts\Queue\QueueDispatcherInterface;
use Waffle\Commons\Contracts\Serializer\Exception\SerializerExceptionInterface;
use Waffle\Commons\Contracts\Telemetry\NullTextMapPropagator;
use Waffle\Commons\Contracts\Telemetry\NullTracer;
use Waffle\Commons\Contracts\Telemetry\TextMapPropagatorInterface;
use Waffle\Commons\Contracts\Telemetry\TracerInterface;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use Waffle\Commons\Queue\Exception\QueueException;

use function array_key_exists;
use function array_key_first;
use function bin2hex;
use function is_array;
use function is_string;
use function max;
use function random_bytes;
use function str_contains;

/**
 * The reference queue driver, on Redis Streams ([QUEUE-02]).
 *
 * Each queue is a stream read through one consumer group, so every message goes
 * to one worker, and a delivery stays pending until the worker settles it.
 * `receive()` first moves the delayed messages that are due into the stream,
 * then reclaims the oldest delivery a worker left unsettled for longer than the
 * claim idle time (`XAUTOCLAIM`) — a crashed worker loses no message, and the
 * crash counts as a failed attempt — and only then reads a new entry.
 * `release()` settles a failed delivery and queues the next attempt in one Lua
 * script, so a crash in between cannot drop the message.
 *
 * Messages are encoded by the {@see EnvelopeCodec} (the serializer's JSON, never
 * `serialize()`); an entry it cannot read is set aside in the queue's
 * `:unreadable` stream instead of blocking the queue. The dispatcher records the
 * current W3C trace context in the envelope.
 *
 * Connections come from the contracts' Redis pool, leased per operation and
 * always released. Stateless between calls: safe in a FrankenPHP worker and in
 * `queue:work`.
 */
final readonly class RedisStreamsQueue implements QueueDispatcherInterface, QueueConsumerInterface
{
    /**
     * Moves the due delayed messages into the stream, atomically.
     * KEYS: the delayed set, the stream. ARGV: now (ms), the batch size.
     */
    private const string PROMOTE = <<<'LUA'
        local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
        for _, envelope in ipairs(due) do
            redis.call('ZREM', KEYS[1], envelope)
            redis.call('XADD', KEYS[2], '*', 'envelope', envelope)
        end
        return #due
        LUA;

    /**
     * Settles a delivery and queues its next attempt, atomically.
     * KEYS: the stream, the delayed set. ARGV: group, entry id, envelope, available at (ms), now (ms).
     */
    private const string RELEASE = <<<'LUA'
        redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
        redis.call('XDEL', KEYS[1], ARGV[2])
        if tonumber(ARGV[4]) > tonumber(ARGV[5]) then
            redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
        else
            redis.call('XADD', KEYS[1], '*', 'envelope', ARGV[3])
        end
        return 1
        LUA;

    public function __construct(
        private RedisConnectionPoolInterface $pool,
        private EnvelopeCodec $codec,
        private ClockInterface $clock,
        private StreamOptions $options,
        private TracerInterface $tracer = new NullTracer(),
        private TextMapPropagatorInterface $propagator = new NullTextMapPropagator(),
    ) {}

    /**
     * @throws QueueException           When the message cannot be encoded or Redis refuses it.
     * @throws InvalidArgumentException When `$queue` is empty.
     * @throws \Random\RandomException  When the platform CSPRNG is unavailable.
     */
    #[\Override]
    public function dispatch(
        MessageInterface $message,
        string $queue = self::DEFAULT_QUEUE,
        ?DateTimeImmutable $availableAt = null,
    ): Envelope {
        $now = $this->clock->now();
        $envelope = new Envelope($this->newId(), $message, $queue, $availableAt ?? $now, 0, $this->traceContext());
        $json = $this->encode($envelope);

        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            $added = $envelope->availableAt > $now
                ? $redis->zAdd(
                    $this->options->delayed($queue),
                    EnvelopeCodec::milliseconds($envelope->availableAt),
                    $json,
                )
                : $redis->xAdd($this->options->stream($queue), '*', ['envelope' => $json]);
            if ($added === false) {
                throw QueueException::refused($redis);
            }
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }

        return $envelope;
    }

    /**
     * @throws QueueException When Redis cannot be read.
     */
    #[\Override]
    public function receive(string $queue, int $timeoutMs = 0): ?Envelope
    {
        $stream = $this->options->stream($queue);
        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            $now = $this->clock->now();
            $promoted =
                $redis->eval(
                    self::PROMOTE,
                    [
                        $this->options->delayed($queue),
                        $stream,
                        (string) EnvelopeCodec::milliseconds($now),
                        (string) $this->options->promoteBatch,
                    ],
                    2,
                ) !== false;
            if (!$promoted) {
                throw QueueException::refused($redis);
            }

            $reclaimed = $this->reclaim($redis, $stream);
            $entry = $reclaimed ?? $this->read($redis, $stream, $timeoutMs);
            if ($entry === null) {
                return null;
            }

            $envelope = $this->codec->decode($entry[1]);
            if ($envelope === null) {
                $this->setAside($redis, $queue, $entry[0], $entry[1]);

                return null;
            }

            // A reclaimed delivery was left unsettled by a worker that stopped: it
            // counts as a failed attempt.
            return ($reclaimed === null ? $envelope : $envelope->withNextAttempt($now))->withReceipt($entry[0]);
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }
    }

    /**
     * @throws QueueException When the envelope has no receipt or Redis refuses the acknowledgement.
     */
    #[\Override]
    public function acknowledge(Envelope $envelope): void
    {
        $receipt = $envelope->receipt ?? throw QueueException::notReceived($envelope);
        $stream = $this->options->stream($envelope->queue);

        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            if (
                $redis->xAck($stream, $this->options->group, [$receipt]) === false
                || $redis->xDel($stream, [$receipt]) === false
            ) {
                throw QueueException::refused($redis);
            }
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }
    }

    /**
     * @throws QueueException When the envelope has no receipt, cannot be encoded, or Redis refuses it.
     */
    #[\Override]
    public function release(Envelope $envelope, DateTimeImmutable $availableAt): void
    {
        $receipt = $envelope->receipt ?? throw QueueException::notReceived($envelope);
        $json = $this->encode($envelope->withNextAttempt($availableAt));

        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            $released =
                $redis->eval(
                    self::RELEASE,
                    [
                        $this->options->stream($envelope->queue),
                        $this->options->delayed($envelope->queue),
                        $this->options->group,
                        $receipt,
                        $json,
                        (string) EnvelopeCodec::milliseconds($availableAt),
                        (string) EnvelopeCodec::milliseconds($this->clock->now()),
                    ],
                    2,
                ) !== false;
            if (!$released) {
                throw QueueException::refused($redis);
            }
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }
    }

    /**
     * The oldest delivery left unsettled longer than the claim idle time, now
     * claimed by this consumer.
     *
     * @return array{string, string}|null The entry id and its envelope.
     *
     * @throws RedisException
     * @throws QueueException When Redis refuses the claim.
     */
    private function reclaim(Redis $redis, string $stream): ?array
    {
        // phpredis answers untyped: the reply is kept in a one-element list and
        // narrowed in place, never bound on its own.
        $claimed = [$redis->xAutoClaim(
            $stream,
            $this->options->group,
            $this->options->consumer,
            $this->options->claimIdleMs,
            '0-0',
            1,
        )];
        if ($claimed[0] === false) {
            if (!str_contains((string) $redis->getLastError(), 'NOGROUP')) {
                throw QueueException::refused($redis);
            }

            // No group yet: read() creates it.
            $redis->clearLastError();

            return null;
        }

        if (!is_array($claimed[0]) || !array_key_exists(1, $claimed[0]) || !is_array($claimed[0][1])) {
            return null;
        }

        return self::firstEntry($claimed[0][1]);
    }

    /**
     * A new entry for this consumer, waiting up to `$timeoutMs` milliseconds;
     * the consumer group is created on first use, from the start of the stream.
     *
     * @return array{string, string}|null The entry id and its envelope.
     *
     * @throws RedisException
     * @throws QueueException When Redis refuses the read.
     */
    private function read(Redis $redis, string $stream, int $timeoutMs): ?array
    {
        // phpredis blocks 1 ms at least (0 would mean forever, a negative value is refused).
        $block = max(1, $timeoutMs);
        $result = [$redis->xReadGroup($this->options->group, $this->options->consumer, [$stream => '>'], 1, $block)];
        if ($result[0] === false && str_contains((string) $redis->getLastError(), 'NOGROUP')) {
            $redis->clearLastError();
            $redis->xGroup('CREATE', $stream, $this->options->group, '0', true);
            // Another worker may have created it first (BUSYGROUP): either way it exists now,
            // and that error must not be reported for a later command.
            $redis->clearLastError();
            $result = [$redis->xReadGroup(
                $this->options->group,
                $this->options->consumer,
                [$stream => '>'],
                1,
                $block,
            )];
        }

        if ($result[0] === false) {
            throw QueueException::refused($redis);
        }

        if (!is_array($result[0]) || !array_key_exists($stream, $result[0]) || !is_array($result[0][$stream])) {
            return null;
        }

        return self::firstEntry($result[0][$stream]);
    }

    /**
     * Moves an entry no allowed message class matches out of the way, to the
     * queue's `:unreadable` stream, and settles it.
     *
     * @throws RedisException
     * @throws QueueException When Redis refuses a command.
     */
    private function setAside(Redis $redis, string $queue, string $entryId, string $json): void
    {
        $stream = $this->options->stream($queue);
        if ($redis->xAdd($this->options->unreadable($queue), '*', [
            'entry' => $json,
        ]) === false || $redis->xAck($stream, $this->options->group, [$entryId]) === false || $redis->xDel($stream, [$entryId]) === false) {
            throw QueueException::refused($redis);
        }
    }

    /**
     * The id and the envelope of the first entry of a stream reply
     * (`[id => ['envelope' => json]]`).
     *
     * @return array{string, string}|null
     */
    private static function firstEntry(array $entries): ?array
    {
        $id = array_key_first($entries);
        if (!is_string($id)) {
            return null;
        }

        $fields = [$entries[$id] ?? null];
        if (!is_array($fields[0]) || !array_key_exists('envelope', $fields[0]) || !is_string($fields[0]['envelope'])) {
            return null;
        }

        return [$id, $fields[0]['envelope']];
    }

    /**
     * @throws QueueException
     */
    private function encode(Envelope $envelope): string
    {
        try {
            return $this->codec->encode($envelope);
        } catch (SerializerExceptionInterface|JsonException $exception) {
            throw QueueException::unencodable($envelope, $exception);
        }
    }

    /**
     * @throws QueueException When the pool has no healthy connection.
     */
    private function acquire(): RedisConnectionInterface
    {
        try {
            return $this->pool->acquire();
        } catch (DatabaseExceptionInterface $exception) {
            throw QueueException::transport($exception);
        }
    }

    /**
     * @throws QueueException When the pool does not hand out an ext-redis client.
     */
    private static function client(RedisConnectionInterface $lease): Redis
    {
        $client = $lease->client();

        return $client instanceof Redis ? $client : throw QueueException::unexpectedClient($client);
    }

    /**
     * @throws \Random\RandomException When the platform CSPRNG is unavailable.
     */
    private function newId(): string
    {
        return bin2hex(random_bytes(16));
    }

    /**
     * The W3C trace context of the code that dispatches.
     *
     * @return array<string, string>
     */
    private function traceContext(): array
    {
        $context = $this->tracer->currentContext();
        if ($context === null) {
            return [];
        }

        $carrier = [];
        $this->propagator->inject($context, $carrier);

        return $carrier;
    }
}
