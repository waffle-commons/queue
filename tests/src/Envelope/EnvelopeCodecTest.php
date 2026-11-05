<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Envelope;

use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Queue\Envelope\EnvelopeCodec;
use WaffleTests\Commons\Queue\AbstractTestCase as TestCase;
use WaffleTests\Commons\Queue\Fixture\InvoiceSerializer;
use WaffleTests\Commons\Queue\Fixture\SendInvoice;

#[CoversClass(EnvelopeCodec::class)]
final class EnvelopeCodecTest extends TestCase
{
    public function testAnEnvelopeSurvivesTheRoundTrip(): void
    {
        $codec = self::codec();
        $envelope = new Envelope(
            'msg-1',
            new SendInvoice('inv-42'),
            'invoices',
            new DateTimeImmutable('2026-10-10T09:00:00.250+00:00'),
            2,
            ['traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'],
            'receipt-ignored',
        );

        $decoded = $codec->decode($codec->encode($envelope));

        static::assertNotNull($decoded);
        static::assertSame('msg-1', $decoded->id);
        static::assertEquals(new SendInvoice('inv-42'), $decoded->message);
        static::assertSame('invoices', $decoded->queue);
        static::assertSame('2026-10-10T09:00:00.250+00:00', $decoded->availableAt->format('Y-m-d\\TH:i:s.vP'));
        static::assertSame(2, $decoded->attempts);
        static::assertSame($envelope->traceContext, $decoded->traceContext);
        static::assertNull($decoded->receipt, 'A receipt belongs to one delivery and is never stored.');
    }

    public function testWritesTheMessageAsTheSerializersJsonAndNoTraceAsAnObject(): void
    {
        $json = self::codec()
            ->encode(new Envelope('msg-1', new SendInvoice('inv-42'), 'default', new DateTimeImmutable('@0')));

        static::assertSame(
            '{"id":"msg-1","type":"WaffleTests\\\\Commons\\\\Queue\\\\Fixture\\\\SendInvoice","queue":"default",'
            . '"payload":"{\\"invoiceId\\":\\"inv-42\\"}","attempts":0,"available_at":0,"trace":{}}',
            $json,
        );
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function unreadableEntries(): iterable
    {
        $valid = [
            'id' => 'msg-1',
            'type' => SendInvoice::class,
            'queue' => 'default',
            'payload' => '{"invoiceId":"inv-42"}',
            'attempts' => 0,
            'available_at' => 0,
            'trace' => [],
        ];

        yield 'not JSON' => ['{oops'];
        yield 'not an object' => ['42'];
        yield 'no id' => [self::json([...$valid, 'id' => ''])];
        yield 'a class no handler handles' => [self::json([...$valid, 'type' => 'App\\Evil'])];
        yield 'a payload the message refuses' => [self::json([...$valid, 'payload' => '{"invoiceId":""}'])];
        yield 'a payload the message fails on unexpectedly' => [self::json([
            ...$valid,
            'payload' => '{"invoiceId":"crash"}',
        ])];
        yield 'mistyped attempts' => [self::json([...$valid, 'attempts' => '2'])];
        yield 'negative attempts' => [self::json([...$valid, 'attempts' => -1])];
        yield 'a trace that is not a map of strings' => [self::json([...$valid, 'trace' => ['traceparent' => 7]])];
        yield 'no trace' => [self::json(array_diff_key($valid, ['trace' => true]))];
    }

    #[DataProvider('unreadableEntries')]
    public function testAnEntryItCannotTrustReadsAsNothing(string $json): void
    {
        static::assertNull(self::codec()->decode($json));
    }

    public function testCountsInstantsInMillisecondsSinceTheEpoch(): void
    {
        $instant = new DateTimeImmutable('2026-10-10T09:00:00.250+00:00');

        static::assertSame(1_791_622_800_250, EnvelopeCodec::milliseconds($instant));
        static::assertEquals($instant, EnvelopeCodec::instant(1_791_622_800_250));
    }

    private static function codec(): EnvelopeCodec
    {
        return new EnvelopeCodec(new InvoiceSerializer(), [SendInvoice::class]);
    }

    private static function json(array $entry): string
    {
        return (string) json_encode($entry);
    }
}
