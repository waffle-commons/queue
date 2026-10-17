<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Fixture;

use DomainException;
use RuntimeException;
use Waffle\Commons\Contracts\Serializer\DtoSerializerInterface;
use Waffle\Commons\Contracts\Serializer\Exception\SerializerExceptionInterface;

/**
 * A serializer for the one fixture message: `{"invoiceId": "…"}`. An empty or
 * missing id is refused, as a DTO hook would; the id `crash` fails otherwise.
 */
final readonly class InvoiceSerializer implements DtoSerializerInterface
{
    #[\Override]
    public function supports(string $class): bool
    {
        return $class === SendInvoice::class;
    }

    #[\Override]
    public function denormalize(array $data, string $class): object
    {
        $id = $data['invoiceId'] ?? null;
        if ($id === 'crash') {
            // A message whose constructor fails with something other than a validation error.
            throw new DomainException('The invoice numbering service is down.');
        }

        $dto = is_string($id) && $id !== '' ? new SendInvoice($id) : null;
        if (!$dto instanceof $class) {
            throw new class('The payload does not fit.') extends RuntimeException implements
                SerializerExceptionInterface {};
        }

        return $dto;
    }

    #[\Override]
    public function deserialize(string $json, string $class): object
    {
        $decoded = json_decode($json, true);

        return $this->denormalize(is_array($decoded) ? $decoded : [], $class);
    }

    #[\Override]
    public function serialize(object $dto): string
    {
        if (!$dto instanceof SendInvoice) {
            throw new class('Not a DTO this serializer handles.') extends RuntimeException implements
                SerializerExceptionInterface {};
        }

        return (string) json_encode(['invoiceId' => $dto->invoiceId]);
    }
}
