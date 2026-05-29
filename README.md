# AwaitGroup

[![Documentation](https://img.shields.io/badge/docs-0.6.0-4d76ae?style=for-the-badge)](https://docs.rs/awaitgroup/0.6.0)
[![Version](https://img.shields.io/crates/v/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
[![License](https://img.shields.io/crates/l/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
[![Actions](https://img.shields.io/github/workflow/status/ibraheemdev/awaitgroup/Rust/master?style=for-the-badge)](https://github.com/ibraheemdev/awaitgroup/actions)

An asynchronous implementation of Go's `sync.WaitGroup`, for Rust async runtimes.

Two variants are provided:

| Type | Model |
|------|--------|
| [`WaitGroup`](https://docs.rs/awaitgroup/latest/awaitgroup/wait_group/struct.WaitGroup.html) | Register work with `add`, finish with `done`, wait until the counter reaches zero. |
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

## Single waiter (`wait` / `wait_arrive`)

`wait()` and `wait_arrive()` may only be awaited from **one** task at a time per instance (one logical “consumer”). The group uses a single `AtomicWaker`; concurrent waiters are not supported.

- **Allowed:** many worker tasks call `done` / `arrive` on clones of the same group, while **one** coordinator task calls `wait` / `wait_arrive`.
- **Not allowed:** two tasks (or two threads) both calling `wait()` or `wait_arrive()` on the same instance, even on clones — the second call panics with `Other threads might still be using it`.

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

The same rule applies to `WaitGroupArrive::wait_arrive`.

## Single-use (no reuse)

Each `WaitGroup` / `WaitGroupArrive` instance is intended for **one** wait cycle only. This keeps the implementation simple and avoids reset logic.

After `wait()` or `wait_arrive()` has been entered (or has completed):

- Do **not** call `wait()` / `wait_arrive()` again on the same instance (panics).
- Do **not** call `add` / `guard_add` / `arrive` while a wait is in progress (panics).
- Do **not** expect a second round of work on the same instance — create a new `WaitGroup::new()` or `WaitGroupArrive::new()` instead.

Typical lifecycle:

1. Create the group.
2. Register all work (`add`, `guard_add`, or spawn tasks that will `arrive`) **before** calling `wait` / `wait_arrive`.
3. Have **one** task call `wait` / `wait_arrive` once.
4. For another batch, allocate a **new** instance.

## Errors

Both types return `anyhow::Result<()>` from their wait methods. Use `done_error` / `arrive_error` to fail the whole group; the first error is retained. After an error, the instance is still single-use (you cannot “reset” and wait again).

## See also

[API documentation on docs.rs](https://docs.rs/awaitgroup) for `WaitGroupWorker`, nested workers, and full method listings.
