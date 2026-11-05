[![Discord](https://img.shields.io/discord/755288001592033391?logo=discord)](https://discord.gg/eKgywnfXr2)
[![PHP Version Require](http://poser.pugx.org/waffle-commons/queue/require/php)](https://packagist.org/packages/waffle-commons/queue)
[![PHP CI](https://github.com/waffle-commons/queue/actions/workflows/main.yml/badge.svg)](https://github.com/waffle-commons/queue/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/waffle-commons/queue/graph/badge.svg)](https://codecov.io/gh/waffle-commons/queue)
[![Latest Stable Version](http://poser.pugx.org/waffle-commons/queue/v)](https://packagist.org/packages/waffle-commons/queue)
[![Latest Unstable Version](http://poser.pugx.org/waffle-commons/queue/v/unstable)](https://packagist.org/packages/waffle-commons/queue)
[![Total Downloads](https://img.shields.io/packagist/dt/waffle-commons/queue.svg)](https://packagist.org/packages/waffle-commons/queue)
[![Packagist License](https://img.shields.io/packagist/l/waffle-commons/queue)](https://github.com/waffle-commons/queue/blob/main/LICENSE.md)

Waffle Queue Component
==========================

> **Release:** `0.1.0-beta7` &nbsp;|&nbsp; [`CHANGELOG.md`](./CHANGELOG.md)
> **Status:** First released in `0.1.0-beta7` — roadmap `[QUEUE-02]` (RFC-015 boundary). The Redis Streams driver and the dead-letter store are in. The contracts live in `waffle-commons/contracts` (`[QUEUE-01]`), and the `queue:work` worker in `waffle-commons/console`.

Background job processing for the Waffle framework: one reference driver on Redis Streams, bounded retries and a
dead-letter store. It is deliberately small — one driver, one worker, at-least-once delivery — and not a Messenger
clone. It is distinct from beta5's finish-request deferral, which runs after the response inside the same request.

## 🧩 What it does

- **`RedisStreams\RedisStreamsQueue`** implements the contracts' `QueueDispatcherInterface` and
  `QueueConsumerInterface` on Redis Streams (Redis ≥ 6.2, `ext-redis`).
  - `dispatch()` adds the message to its queue's stream, or to the queue's delayed set when it is due later. The
    envelope records the current W3C trace context.
  - `receive()` first moves the delayed messages that are due into the stream. It then reclaims a delivery that a
    worker left unsettled for longer than `claimIdleMs` (`XAUTOCLAIM`), so a crashed worker loses no message; the
    crash counts as a failed attempt. Only then does it read a new entry (`XREADGROUP`, waiting up to the timeout).
  - `acknowledge()` removes the entry. `release()` settles a failed delivery and queues the next attempt, at once or
    delayed, in one Lua script: a crash between the two steps cannot drop the message.
- **`RedisStreams\RedisFailedMessageStore`** implements `FailedMessageStoreInterface`: each failed message is kept
  with its full envelope (id, attempts, trace context), the reason and the failure time, oldest first.
- **`Envelope\EnvelopeCodec`** writes an envelope as JSON, the message through the contracts'
  `DtoSerializerInterface` (`waffle-commons/serializer`). It reads one back only into an allowed message class, and
  through the class constructor, so that its hooks validate the payload again. It never calls `unserialize()`: a
  queue is an input channel. An entry it cannot read is set aside and acknowledged, so it never blocks the queue.
- **`Health\StreamsHealthCheck`** is the `/readyz` probe for the driver (OPS-01): one `XLEN` on the queue's own stream
  through a leased connection, so it exercises the driver's key and permissions in one read round trip and reports
  how many messages wait. Critical by default; a failure is reported down with the reason, never thrown.

Connections come from the contracts' `RedisConnectionPoolInterface`, leased per operation and always released. The
classes keep no state between calls.

## 🔑 Redis keys

| Key | Type | Holds |
|---|---|---|
| `{prefix}{queue}` | stream, consumer group `waffle` | the messages ready for delivery, and the deliveries in flight |
| `{prefix}{queue}:delayed` | sorted set, scored by due time | delayed messages and retries waiting for their back-off |
| `{prefix}{queue}:unreadable` | stream | the entries the codec refused, for inspection |
| `{prefix}failed`, `{prefix}failed:order` | hash, sorted set | the dead-letter store |

`prefix` defaults to `waffle:queue:` (`StreamOptions::DEFAULT_PREFIX`).

## 🚀 Usage

The application builds the driver where it registers its console commands (`bin/waffle`), and hands it to the
`queue:work` worker of `waffle-commons/console`. The worker resets the container after every message, recycles on a
message count or a memory ceiling, and drains on SIGTERM and SIGINT (through `pcntl`): it stops taking messages,
finishes the one in hand, then exits:

```php
$handlers = [new SendInvoiceHandler($invoices)];          // MessageHandlerInterface, one per message class
$codec = new EnvelopeCodec(
    serializer: $serializer,                              // DtoSerializerInterface
    messages: array_map(static fn(MessageHandlerInterface $handler): string => $handler->handles(), $handlers),
);
$queue = new RedisStreamsQueue(
    pool: $redisPool,                                     // RedisConnectionPoolInterface (phpredis clients)
    codec: $codec,
    clock: new SystemClock(),
    options: new StreamOptions(consumer: gethostname() ?: 'worker'),
);

$app->add(new QueueWorkCommand(
    consumer: $queue,
    failed: new RedisFailedMessageStore($redisPool, $codec, new SystemClock()),
    handlers: $handlers,
    resettable: $container,
    clock: new SystemClock(),
));
```

Code that starts background work depends on `QueueDispatcherInterface` only:
`$dispatcher->dispatch(new SendInvoice($invoiceId))`.

- **The handled classes are the allow-list.** Give the codec the classes the handlers declare: the worker then
  never rebuilds a class that nobody handles.
- **One consumer name per host, not per process.** Redis keeps every consumer name in the group until it is
  deleted, and the worker recycles often; a name per container keeps the group bounded.

## ⚠️ Delivery guarantees

- **At least once.** A handler is idempotent, or de-duplicates on `$envelope->id`.
- **Keep handlers shorter than `claimIdleMs`** (60 s by default). A delivery still running after that is reclaimed
  by another worker and delivered a second time.
- **Deploy the handlers before the code that dispatches a new message class.** A worker that does not know the class
  sets the entry aside as unreadable instead of handling it.
- **One Redis server (or a Sentinel-managed primary), not Redis Cluster.** The release script writes the stream and
  the delayed set in one call; in a cluster, the two keys would have to share a hash slot.

## 📦 Installation

```bash
composer require waffle-commons/queue
```

## 🧭 Architectural boundary (`mago guard`)

`Waffle\Commons\Queue` depends only on `waffle-commons/contracts` and the PSR clock interface (`psr/clock`) —
never on a sibling component's concrete classes. The serializer, the Redis pool and the tracer all come in through
contracts. Every interface it implements lands in `waffle-commons/contracts` first.

## 🧵 Worker safety

Services are resident in FrankenPHP worker mode: no mutable state may survive a request. Anything that
holds per-request state implements `ResettableInterface` directly; `composer igor` must report 0 KO.

## 🧪 Testing

```bash
composer mago    # formatter, linter, analyzer, guard — clean means zero output
composer tests   # PHPUnit 12.5, line coverage ≥ 95 %
composer igor    # worker-safety audit
```

The driver tests run against a real Redis: `WAFFLE_TEST_REDIS_HOST` (default `waffle-redis`, the development
stack's service). They are skipped when it cannot be reached — which the definition of done does not accept.

## 📚 Documentation

Full documentation tree: [waffle-commons/documentation](https://github.com/waffle-commons/documentation)

## 📄 License

MIT — see [`LICENSE.md`](./LICENSE.md).
