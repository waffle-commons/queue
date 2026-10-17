<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\RedisStreams;

use InvalidArgumentException;

/**
 * Where and how the Redis Streams queue keeps its messages ([QUEUE-02]).
 *
 * Each queue is one stream (`{prefix}{queue}`) read by one consumer group;
 * delayed messages wait in a sorted set (`{prefix}{queue}:delayed`) until they
 * are due, and entries no allowed message class matches are set aside in
 * `{prefix}{queue}:unreadable`. The consumer name identifies one worker process
 * within the group: give each process its own (host name and process id).
 */
final class StreamOptions
{
    public const string DEFAULT_PREFIX = 'waffle:queue:';

    public const string DEFAULT_GROUP = 'waffle';

    /** How long a delivery may stay unacknowledged before another worker reclaims it. */
    public const int DEFAULT_CLAIM_IDLE_MS = 60_000;

    /** How many due delayed messages one receive moves into the stream. */
    public const int DEFAULT_PROMOTE_BATCH = 100;

    /**
     * @throws InvalidArgumentException When a name is empty or a bound is not positive.
     */
    public function __construct(
        public private(set) string $consumer {
            set(string $value) {
                $this->consumer = self::name($value, 'consumer');
            }
        },
        public private(set) string $prefix = self::DEFAULT_PREFIX {
            set(string $value) {
                $this->prefix = self::name($value, 'prefix');
            }
        },
        public private(set) string $group = self::DEFAULT_GROUP {
            set(string $value) {
                $this->group = self::name($value, 'group');
            }
        },
        public private(set) int $claimIdleMs = self::DEFAULT_CLAIM_IDLE_MS {
            set(int $value) {
                $this->claimIdleMs = self::positive($value, 'claimIdleMs');
            }
        },
        public private(set) int $promoteBatch = self::DEFAULT_PROMOTE_BATCH {
            set(int $value) {
                $this->promoteBatch = self::positive($value, 'promoteBatch');
            }
        },
    ) {}

    public function stream(string $queue): string
    {
        return $this->prefix . $queue;
    }

    public function delayed(string $queue): string
    {
        return $this->prefix . $queue . ':delayed';
    }

    public function unreadable(string $queue): string
    {
        return $this->prefix . $queue . ':unreadable';
    }

    /**
     * @throws InvalidArgumentException
     */
    private static function name(string $value, string $option): string
    {
        if ($value === '') {
            throw new InvalidArgumentException('The stream option "' . $option . '" must not be empty.');
        }

        return $value;
    }

    /**
     * @throws InvalidArgumentException
     */
    private static function positive(int $value, string $option): int
    {
        if ($value < 1) {
            throw new InvalidArgumentException('The stream option "' . $option . '" must be at least 1.');
        }

        return $value;
    }
}
