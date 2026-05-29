# AwaitGroup

[![Documentation](https://img.shields.io/badge/docs-0.6.0-4d76ae?style=for-the-badge)](https://docs.rs/awaitgroup/0.6.0)
[![Version](https://img.shields.io/crates/v/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
[![License](https://img.shields.io/crates/l/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
[![Actions](https://img.shields.io/github/workflow/status/ibraheemdev/awaitgroup/Rust/master?style=for-the-badge)](https://github.com/ibraheemdev/awaitgroup/actions)

An asynchronous implementation of Go's `sync.WaitGroup`, for Rust async runtimes.

Two variants are provided:

| Type | Model |
|------|--------|
| [`WaitGroup`](https://docs.rs/awaitgroup/latest/awaitgroup/wait_group/struct.WaitGroup.html) | Register work with `add`, finish with `done`, `wait` until the counter reaches zero. |
| [`WaitGroupArrive`](https://docs.rs/awaitgroup/latest/awaitgroup/wait_group_arrive/struct.WaitGroupArrive.html) | Workers call `arrive` to increment a counter; `wait_arrive(n)` completes when `n` arrivals have occurred. |

## Quick start — `WaitGroup`

```rust
use awaitgroup::wait_group::WaitGroup;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let wg = WaitGroup::new();

    for _ in 0..5 {
        let wg = wg.clone();
        wg.add();
        tokio::spawn(async move {
            // do work...
            wg.done();
        });
    }

    wg.wait().await?;
    Ok(())
}
```

`guard_add` decrements the counter automatically when the guard is dropped (same idea as a scope guard):

```rust
let guard = wg.guard_add();
tokio::spawn(async move {
    // do work...
    drop(guard);
});
wg.wait().await?;
```

Register all `add` / `guard_add` calls **before** `wait()` — see [WaitGroup rules](#waitgroup-rules) below.

## Quick start — `WaitGroupArrive`

Use this when you know how many completions you need, but tasks only signal arrival (no paired `done`):

```rust
use awaitgroup::wait_group_arrive::WaitGroupArrive;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let wg = WaitGroupArrive::new();

    for _ in 0..5 {
        let wg = wg.clone();
        tokio::spawn(async move {
            wg.arrive();
        });
    }

    wg.wait_arrive(5).await?;
    Ok(())
}
```

You may call `wait_arrive` again with the **same** target `n` after a previous wait has finished (for example, to re-check that `count >= n`). See [WaitGroupArrive rules](#waitgrouparrive-rules).

## Single waiter (`wait` / `wait_arrive`)

`wait()` and `wait_arrive()` may only be awaited from **one** task at a time per instance (one logical “consumer”). The group uses a single `AtomicWaker`; concurrent waiters are not supported.

When the wait future completes, `unlock_waiting` releases the waiter lock so another wait can be started later — but only **sequentially**, never in parallel.

- **Allowed:** many worker tasks call `done` / `arrive` on clones of the same group, while **one** coordinator task calls `wait` / `wait_arrive`.
- **Not allowed:** two tasks both awaiting `wait()` or `wait_arrive()` at the same time — the second call panics with `Other threads might still be using it`.

```rust
// OK: one waiter, many workers
let wg = WaitGroup::new();
for _ in 0..5 {
    let wg = wg.clone();
    wg.add();
    tokio::spawn(async move { wg.done(); });
}
wg.wait().await?; // only this task waits

// Panics: two concurrent waits
let wg2 = wg.clone();
tokio::spawn(async move { wg2.wait().await.unwrap() });
wg.wait().await?; // panic
```

## `WaitGroup` rules

After `wait()` is first called, the group enters a **closing** phase (`is_closing`):

| Action | Allowed? |
|--------|----------|
| `done` / `done_error` on work already registered | Yes (including while `wait` is pending) |
| `add` / `guard_add` after `wait` has started | No — panics with `WaitGroup::add called during wait` |
| Second `wait()` while another `wait` is in progress | No — panics |
| Second `wait()` **after** the first returned and `count == 0` | Yes |

Typical lifecycle:

1. Create the group.
2. Register all work with `add` / `guard_add` (or `worker().add()`).
3. Call `wait()` once (or again later only if `count` is already zero).
4. To run a **new** batch of work with fresh `add` calls, create a new `WaitGroup::new()`.

## `WaitGroupArrive` rules

`wait_arrive(n)` uses a waiter lock (`lock_waiting` / `unlock_waiting`) like `wait()`, and tracks the target `n` in `wait_arrive_count`.

| Action | Allowed? |
|--------|----------|
| `arrive` / `arrive_num` while `wait_arrive` is pending (if `count` does not exceed `n`) | Yes |
| `arrive` after `count` already equals `n` (would push past the target) | No — panics |
| Second `wait_arrive(n)` with the **same** `n` after the first returned | Yes — returns immediately if `count >= n` |
| `wait_arrive(m)` with `m != n` after a target was already set | No — panics |
| Two concurrent `wait_arrive` calls | No — panics |

```rust
let wg = WaitGroupArrive::new();
// ... spawn tasks that call wg.arrive() ...
wg.wait_arrive(3).await?;

// OK: same target again (e.g. barrier-style re-check)
wg.wait_arrive(3).await?;

// Panics: different target
wg.wait_arrive(5).await?;
```

The cumulative `count` never decreases; it only grows. A new batch that needs a **larger** target or more `arrive` calls should use a new `WaitGroupArrive::new()`.

## Errors

Both types return `anyhow::Result<()>` from their wait methods. Use `done_error` / `arrive_error` to fail the whole group; the **first** error is kept.

- **`WaitGroup`:** after an error, you can call `wait()` again only if `count` is already zero; you still cannot call `add` after the first `wait`.
- **`WaitGroupArrive`:** you can call `wait_arrive(n)` again with the same `n`; it will keep returning the stored error until you use a new instance.

## See also

[API documentation on docs.rs](https://docs.rs/awaitgroup) for `WaitGroupWorker`, nested workers, and full method listings.
