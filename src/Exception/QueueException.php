<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\Exception;

use Redis;
use RuntimeException;
use Throwable;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\Exception\QueueExceptionInterface;

use function sprintf;

/**
 * A queue transport failure: Redis is unreachable or refused a command, the pool
 * hands out something else than an ext-redis client, or a message cannot be
 * encoded ([QUEUE-02]).
 */
final class QueueException extends RuntimeException implements QueueExceptionInterface
{
    public static function transport(Throwable $previous): self
    {
        return new self('The queue transport failed: ' . $previous->getMessage(), 0, $previous);
    }

    /**
     * Redis answered a command with an error instead of throwing: phpredis
     * reports server errors (a key of the wrong type, a failing script) as a
     * `false` reply and the last error.
     */
    public static function refused(Redis $redis): self
    {
        $error = $redis->getLastError();
        $redis->clearLastError();

        return new self('Redis refused the command: ' . ($error ?? 'no reason given') . '.');
    }

    public static function unexpectedClient(object $client): self
    {
        return new self(sprintf('The Redis pool hands out a "%s", not an ext-redis client.', $client::class));
    }

    public static function notReceived(Envelope $envelope): self
    {
        return new self(sprintf('Message "%s" has no receipt: it was not received from this queue.', $envelope->id));
    }

    public static function unencodable(Envelope $envelope, Throwable $previous): self
    {
        return new self(
            sprintf('Message "%s" cannot be encoded: %s', $envelope->id, $previous->getMessage()),
            0,
            $previous,
        );
    }
}
