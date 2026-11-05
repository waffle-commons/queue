<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Fixture;

use DateTimeImmutable;
use Psr\Clock\ClockInterface;

/** A clock the test moves. */
final class MovableClock implements ClockInterface
{
    public function __construct(
        public DateTimeImmutable $now = new DateTimeImmutable('2026-10-10T09:00:00+00:00'),
    ) {}

    #[\Override]
    public function now(): DateTimeImmutable
    {
        return $this->now;
    }

    public function advance(string $by): void
    {
        $this->now = $this->now->modify($by);
    }
}
