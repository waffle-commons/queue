<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Exception;

use DateTimeImmutable;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use stdClass;
use Waffle\Commons\Contracts\Queue\Envelope;
use Waffle\Commons\Contracts\Queue\Exception\QueueExceptionInterface;
use Waffle\Commons\Queue\Exception\QueueException;
use WaffleTests\Commons\Queue\AbstractTestCase as TestCase;
use WaffleTests\Commons\Queue\Fixture\SendInvoice;

#[CoversClass(QueueException::class)]
final class QueueExceptionTest extends TestCase
{
    public function testNamesWhatWentWrong(): void
    {
        $previous = new RuntimeException('Connection refused');
        $envelope = new Envelope('msg-1', new SendInvoice('inv-42'), 'default', new DateTimeImmutable());

        static::assertInstanceOf(QueueExceptionInterface::class, QueueException::transport($previous));
        static::assertSame(
            'The queue transport failed: Connection refused',
            QueueException::transport($previous)->getMessage(),
        );
        static::assertSame($previous, QueueException::transport($previous)->getPrevious());
        static::assertSame(
            'The Redis pool hands out a "stdClass", not an ext-redis client.',
            QueueException::unexpectedClient(new stdClass())->getMessage(),
        );
        static::assertSame(
            'Message "msg-1" has no receipt: it was not received from this queue.',
            QueueException::notReceived($envelope)->getMessage(),
        );
        static::assertSame(
            'Message "msg-1" cannot be encoded: Connection refused',
            QueueException::unencodable($envelope, $previous)->getMessage(),
        );
    }
}
