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

> **Release:** _unreleased_ &nbsp;|&nbsp; [`CHANGELOG.md`](./CHANGELOG.md)
> **Status:** In development for `0.1.0-beta7` — roadmap `[QUEUE-02]` (RFC-015 boundary). The queue contracts land in `waffle-commons/contracts` first (`[QUEUE-01]`), and the `queue:work` worker command lives in `waffle-commons/console`.

Background job processing for the Waffle framework: a Redis Streams driver with bounded retries and a dead-letter store.

## 📦 Installation

```bash
composer require waffle-commons/queue
```

## 🧭 Architectural boundary (`mago guard`)

`Waffle\Commons\Queue` depends only on `waffle-commons/contracts` (plus `waffle-commons/utils`
for shared primitives) and the PSR interfaces — never on a sibling component's concrete classes. Every
interface it implements lands in `waffle-commons/contracts` first.

## 🧵 Worker safety

Services are resident in FrankenPHP worker mode: no mutable state may survive a request. Anything that
holds per-request state implements `ResettableInterface` directly; `composer igor` must report 0 KO.

## 🧪 Testing

```bash
composer mago    # formatter, linter, analyzer, guard — clean means zero output
composer tests   # PHPUnit 12.5, line coverage ≥ 95 %
composer igor    # worker-safety audit
```

## 📚 Documentation

Full documentation tree: [waffle-commons/documentation](https://github.com/waffle-commons/documentation)

## 📄 License

MIT — see [`LICENSE.md`](./LICENSE.md).
