<?php

declare(strict_types=1);

namespace Waffle\Commons\Queue\Envelope;

use DateTimeImmutable;
use InvalidArgumentException;
use JsonException;
use stdClass;
use Throwable;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\MessageInterface;
use Waffle\Commons\Contracts\Serializer\DtoSerializerInterface;
use Waffle\Commons\Contracts\Serializer\Exception\SerializerExceptionInterface;

use function array_key_exists;
use function array_keys;
use function floor;
use function is_array;
use function is_int;
use function is_string;
use function json_decode;
use function json_encode;
use function sprintf;

/**
 * Writes an envelope as JSON for a queue, and reads it back ([QUEUE-02]).
 *
 * The message travels as the serializer's JSON (the contracts'
 * `DtoSerializerInterface`), next to its class name, the envelope's id, queue,
 * failed attempts, availability instant (milliseconds) and trace context. Reading
 * rebuilds the message only for an allowed class, through its constructor, so
 * its validation runs again; anything else — malformed JSON, an unknown class, a
 * payload the message refuses — reads as nothing. A queue is an input channel:
 * nothing is ever `unserialize()`d.
 */
final readonly class EnvelopeCodec
{
    /**
     * @param list<class-string<MessageInterface>> $messages The message classes it rebuilds (those a handler handles).
     */
    public function __construct(
        private DtoSerializerInterface $serializer,
        private array $messages,
    ) {}

    /**
     * @throws SerializerExceptionInterface When the message is not a DTO the serializer handles.
     * @throws JsonException                When the envelope cannot be encoded.
     */
    public function encode(Envelope $envelope): string
    {
        return json_encode(
            [
                'id' => $envelope->id,
                'type' => $envelope->message::class,
                'queue' => $envelope->queue,
                'payload' => $this->serializer->serialize($envelope->message),
                'attempts' => $envelope->attempts,
                'available_at' => self::milliseconds($envelope->availableAt),
                'trace' => $envelope->traceContext === [] ? new stdClass() : $envelope->traceContext,
            ],
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
        );
    }

    /**
     * The envelope a queue entry holds, or null when the entry is not one this
     * codec reads.
     */
    public function decode(string $json): ?Envelope
    {
        try {
            // json_decode() returns its result untyped: it is kept in a one-element
            // list and narrowed in place, never bound on its own.
            $decoded = [json_decode($json, true, 16, JSON_THROW_ON_ERROR)];
        } catch (JsonException) {
            return null;
        }

        if (!is_array($decoded[0])) {
            return null;
        }

        $entry = $decoded[0];
        $id = self::text($entry, 'id');
        $type = self::text($entry, 'type');
        $queue = self::text($entry, 'queue');
        $payload = self::text($entry, 'payload');
        $attempts = self::integer($entry, 'attempts');
        $availableAt = self::integer($entry, 'available_at');
        $trace = self::trace($entry);
        if (
            $id === null
            || $type === null
            || $queue === null
            || $payload === null
            || $attempts === null
            || $availableAt === null
            || $trace === null
        ) {
            return null;
        }

        $message = $this->rebuild($type, $payload);
        $instant = self::instant($availableAt);
        if ($message === null || $instant === null) {
            return null;
        }

        try {
            return new Envelope($id, $message, $queue, $instant, $attempts, $trace);
        } catch (InvalidArgumentException) {
            return null;
        }
    }

    /**
     * An instant as whole milliseconds since the Unix epoch.
     */
    public static function milliseconds(DateTimeImmutable $instant): int
    {
        return (int) $instant->format('Uv');
    }

    /**
     * The UTC instant a number of milliseconds since the Unix epoch stands for.
     */
    public static function instant(int $milliseconds): ?DateTimeImmutable
    {
        $seconds = (int) floor($milliseconds / 1000);
        $instant = DateTimeImmutable::createFromFormat('U.v', sprintf(
            '%d.%03d',
            $seconds,
            $milliseconds - ($seconds * 1000),
        ));

        return $instant === false ? null : $instant;
    }

    private function rebuild(string $type, string $payload): ?MessageInterface
    {
        foreach ($this->messages as $class) {
            if ($class !== $type) {
                continue;
            }

            try {
                return $this->serializer->deserialize($payload, $class);
            } catch (Throwable) {
                // A queue is an input channel: whatever a payload makes the message class
                // throw, validation error or not, the entry is unreadable. It must never
                // stop the worker reading it, or every worker would stop on it in turn.
                return null;
            }
        }

        return null;
    }

    private static function text(array $entry, string $key): ?string
    {
        return array_key_exists($key, $entry) && is_string($entry[$key]) && $entry[$key] !== '' ? $entry[$key] : null;
    }

    private static function integer(array $entry, string $key): ?int
    {
        return array_key_exists($key, $entry) && is_int($entry[$key]) ? $entry[$key] : null;
    }

    /**
     * The trace context: string keys and string values only.
     *
     * @return array<string, string>|null
     */
    private static function trace(array $entry): ?array
    {
        if (!array_key_exists('trace', $entry) || !is_array($entry['trace'])) {
            return null;
        }

        $raw = $entry['trace'];
        $trace = [];
        foreach (array_keys($raw) as $key) {
            if (!is_string($key) || !array_key_exists($key, $raw) || !is_string($raw[$key])) {
                return null;
            }

            $trace[$key] = $raw[$key];
        }

        return $trace;
    }
}
