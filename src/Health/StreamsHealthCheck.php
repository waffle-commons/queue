<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\Health;

use Redis;
use Throwable;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionPoolInterface;
use Waffle\Commons\Contracts\Health\HealthCheckInterface;
use Waffle\Commons\Contracts\Health\HealthResult;
use Waffle\Commons\Contracts\Queue\QueueDispatcherInterface;
use Waffle\Commons\Queue\RedisStreams\StreamOptions;

/**
 * Probes the Redis Streams driver for `/readyz` ([OPS-01], RFC-023 §3.3): one `XLEN` on the queue's own
 * stream, through a leased connection. It exercises the key and the permissions the driver uses, costs
 * one read round trip, and reports how many messages wait. Critical by default, as it is for consumers.
 */
final readonly class StreamsHealthCheck implements HealthCheckInterface
{
    public function __construct(
        private RedisConnectionPoolInterface $pool,
        private StreamOptions $options,
        private string $queue = QueueDispatcherInterface::DEFAULT_QUEUE,
        private string $name = 'queue',
        private bool $critical = true,
    ) {}

    #[\Override]
    public function name(): string
    {
        return $this->name;
    }

    #[\Override]
    public function isCritical(): bool
    {
        return $this->critical;
    }

    #[\Override]
    public function check(): HealthResult
    {
        // The contract forbids throwing: every failure below is reported as down, with the reason.
        try {
            $lease = $this->pool->acquire();
        } catch (Throwable $e) {
            return HealthResult::down('The queue could not lease a Redis connection: ' . $e->getMessage());
        }

        try {
            $client = $lease->client();
            if (!$client instanceof Redis) {
                return HealthResult::down('The pool did not hand out an ext-redis client.');
            }

            $length = $client->xLen($this->options->stream($this->queue));

            return is_int($length)
                ? HealthResult::up($length . ' message(s) waiting')
                : HealthResult::down('Redis refused XLEN on the queue stream.');
        } catch (Throwable $e) {
            return HealthResult::down('Redis failed on the queue stream: ' . $e->getMessage());
        } finally {
            $this->pool->release($lease);
        }
    }
}
