<?php

declare(strict_types=1);

namespace WaffleTests\Commons\Queue\Fixture;

use Waffle\Commons\Contracts\Queue\MessageInterface;

final readonly class SendInvoice implements MessageInterface
{
    public function __construct(
        public string $invoiceId,
    ) {}
}
