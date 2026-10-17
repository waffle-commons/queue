<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Fixture;

use Closure;
use Waffle\Commons\Contracts\Data\Connection\ConnectionInterface;
use Waffle\Commons\Contracts\Data\Connection\ConnectionKind;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionInterface;
use Waffle\Commons\Contracts\Data\Connection\RedisConnectionPoolInterface;

/**
 * A pool that leases one client, built on demand, and counts its leases.
 */
final class RedisPool implements RedisConnectionPoolInterface
{
    public int $acquired = 0;

    public int $released = 0;

    /**
     * @param Closure(): object $client
     */
    public function __construct(
        private readonly Closure $client,
    ) {}

    #[\Override]
    public function acquire(): RedisConnectionInterface
    {
        ++$this->acquired;
        $client = ($this->client)();

        return new readonly class($client) implements RedisConnectionInterface {
            public function __construct(
                private object $client,
            ) {}

            #[\Override]
            public function client(): object
            {
                return $this->client;
            }

            #[\Override]
            public function kind(): ConnectionKind
            {
                return ConnectionKind::Redis;
            }

            #[\Override]
            public function isAlive(): bool
            {
                return true;
            }

            #[\Override]
            public function id(): int
            {
                return spl_object_id($this->client);
            }
        };
    }

    #[\Override]
    public function release(ConnectionInterface $connection): void
    {
        ++$this->released;
    }
}
