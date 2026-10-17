<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Fixture;

use RuntimeException;
use Waffle\Commons\Contracts\Data\Exception\DatabaseExceptionInterface;

/** A pool that cannot hand out a healthy connection. */
final class UnhealthyPoolException extends RuntimeException implements DatabaseExceptionInterface
{
    public function __construct()
    {
        parent::__construct('No healthy client.');
    }

    #[\Override]
    public function getSqlState(): ?string
    {
        return null;
    }
}
