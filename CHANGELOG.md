# Changelog — waffle-commons/queue

All notable changes to this component are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Released in lockstep with the Waffle Commons umbrella tag.

## [Unreleased]

### Added
- Initial scaffold from `waffle-commons/component-template`: CI pipeline (Mago, Igor, PHPUnit,
  TruffleHog, SBOM, Psalm taint analysis), Dependabot, Mago zero-baseline configuration.
- **Redis Streams driver ([QUEUE-02]).** `RedisStreams\RedisStreamsQueue` implements the contracts'
  `QueueDispatcherInterface` and `QueueConsumerInterface`: one stream and one consumer group per queue, a sorted set
  for delayed messages, `XAUTOCLAIM` reclaim of the deliveries a crashed worker left pending (the crash counts as a
  failed attempt), and an atomic Lua `release()`. The dispatcher records the W3C trace context in the envelope.
  `RedisStreams\StreamOptions` holds the consumer name, key prefix, group, claim idle time and promotion batch.
- `RedisStreams\RedisFailedMessageStore` — the dead-letter store: the full envelope, the reason and the failure
  time of every message given up on, oldest first.
- `Envelope\EnvelopeCodec` — envelopes as JSON, messages through the contracts' `DtoSerializerInterface`; rebuilds
  only allow-listed message classes, through their constructors, never with `unserialize()`. An entry it cannot
  read is set aside on the queue's `:unreadable` stream.
- `Exception\QueueException` (implements `QueueExceptionInterface`) for transport failures and refused commands.
- Integration tests against a real Redis (`WAFFLE_TEST_REDIS_HOST`, default `waffle-redis`), including a worker
  crash in mid-message and the dead-letter store.

- `Health\StreamsHealthCheck` (OPS-01, RFC-023 §3.3) — the `/readyz` probe for the Redis Streams driver: one `XLEN` on the queue's own stream through a leased connection, so it exercises the driver's key and permissions in one read round trip and reports how many messages wait. Critical by default; failures are reported down with the reason, never thrown.
### Dependencies
- `ext-redis` and `psr/clock` are direct requirements.
