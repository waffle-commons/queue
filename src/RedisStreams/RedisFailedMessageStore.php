<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\RedisStreams;

use JsonException;
use Psr\Clock\ClockInterface;
use Redis;
use RedisException;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionInterface;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionPoolInterface;
use Waffle\Commons\Contracts\Data\Exception\DatabaseExceptionInterface;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\FailedMessage;
use Waffle\Commons\Contracts\Queue\FailedMessageStoreInterface;
use Waffle\Commons\Contracts\Serializer\Exception\SerializerExceptionInterface;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use Waffle\Commons\Queue\Exception\QueueException;

use function array_key_exists;
use function array_keys;
use function is_array;
use function is_int;
use function is_string;
use function json_decode;
use function json_encode;

/**
 * The dead-letter store on Redis ([QUEUE-02]): a hash of failed messages by id
 * (`{prefix}failed`), each with its full envelope, the failure reason and the
 * instant, and a sorted set (`{prefix}failed:order`) that lists them oldest
 * first.
 *
 * A message stored again (it failed after being dispatched once more) replaces
 * its earlier record. A record the codec can no longer read — its message class
 * is no longer handled — is left in place and skipped by `list()`.
 */
final readonly class RedisFailedMessageStore implements FailedMessageStoreInterface
{
    public function __construct(
        private RedisConnectionPoolInterface $pool,
        private EnvelopeCodec $codec,
        private ClockInterface $clock,
        private string $prefix = StreamOptions::DEFAULT_PREFIX,
    ) {}

    /**
     * @throws QueueException When the envelope cannot be encoded or Redis refuses it.
     */
    #[\Override]
    public function store(Envelope $envelope, string $reason): void
    {
        $failedAt = EnvelopeCodec::milliseconds($this->clock->now());
        try {
            $record = json_encode(
                ['envelope' => $this->codec->encode($envelope), 'reason' => $reason, 'failed_at' => $failedAt],
                JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
            );
        } catch (SerializerExceptionInterface|JsonException $exception) {
            throw QueueException::unencodable($envelope, $exception);
        }

        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            if (
                $redis->hSet($this->records(), $envelope->id, $record) === false
                || $redis->zAdd($this->order(), $failedAt, $envelope->id) === false
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
     * @throws QueueException When Redis cannot be read.
     */
    #[\Override]
    public function list(int $limit = 100): array
    {
        if ($limit < 1) {
            return [];
        }

        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            $order = [$redis->zRange($this->order(), 0, $limit - 1)];
            if ($order[0] === false) {
                throw QueueException::refused($redis);
            }

            $ids = self::strings($order);
            if ($ids === []) {
                return [];
            }

            $records = [$redis->hMGet($this->records(), $ids)];
            if ($records[0] === false) {
                throw QueueException::refused($redis);
            }
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }

        if (!is_array($records[0])) {
            return [];
        }

        $failed = [];
        foreach ($ids as $id) {
            if (!array_key_exists($id, $records[0]) || !is_string($records[0][$id])) {
                continue;
            }

            $message = $this->decode($records[0][$id]);
            if ($message !== null) {
                $failed[] = $message;
            }
        }

        return $failed;
    }

    /**
     * @throws QueueException When Redis refuses it.
     */
    #[\Override]
    public function remove(string $id): void
    {
        $lease = $this->acquire();
        try {
            $redis = self::client($lease);
            if ($redis->hDel($this->records(), $id) === false || $redis->zRem($this->order(), $id) === false) {
                throw QueueException::refused($redis);
            }
        } catch (RedisException $exception) {
            throw QueueException::transport($exception);
        } finally {
            $this->pool->release($lease);
        }
    }

    private function records(): string
    {
        return $this->prefix . 'failed';
    }

    private function order(): string
    {
        return $this->prefix . 'failed:order';
    }

    private function decode(string $record): ?FailedMessage
    {
        try {
            // json_decode() returns its result untyped: it is kept in a one-element
            // list and narrowed in place, never bound on its own.
            $decoded = [json_decode($record, true, 4, JSON_THROW_ON_ERROR)];
        } catch (JsonException) {
            return null;
        }

        if (
            !is_array($decoded[0])
            || !array_key_exists('envelope', $decoded[0])
            || !is_string($decoded[0]['envelope'])
            || !array_key_exists('reason', $decoded[0])
            || !is_string($decoded[0]['reason'])
            || !array_key_exists('failed_at', $decoded[0])
            || !is_int($decoded[0]['failed_at'])
        ) {
            return null;
        }

        $envelope = $this->codec->decode($decoded[0]['envelope']);
        $failedAt = EnvelopeCodec::instant($decoded[0]['failed_at']);
        if ($envelope === null || $failedAt === null) {
            return null;
        }

        return new FailedMessage($envelope, $decoded[0]['reason'], $failedAt);
    }

    /**
     * The strings of a reply list, in order.
     *
     * @param array{0: Redis|array|false} $reply
     *
     * @return list<string>
     */
    private static function strings(array $reply): array
    {
        if (!is_array($reply[0])) {
            return [];
        }

        $strings = [];
        foreach (array_keys($reply[0]) as $key) {
            if (!array_key_exists($key, $reply[0]) || !is_string($reply[0][$key])) {
                continue;
            }

            $strings[] = $reply[0][$key];
        }

        return $strings;
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
}
