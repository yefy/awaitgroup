//! [![Documentation](https://img.shields.io/badge/docs-0.6.0-4d76ae?style=for-the-badge)](https://docs.rs/awaitgroup/0.6.0)
//! [![Version](https://img.shields.io/crates/v/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
//! [![License](https://img.shields.io/crates/l/awaitgroup?style=for-the-badge)](https://crates.io/crates/awaitgroup)
//! [![Actions](https://img.shields.io/github/workflow/status/ibraheemdev/awaitgroup/Rust/master?style=for-the-badge)](https://github.com/ibraheemdev/awaitgroup/actions)
//!
//! An asynchronous implementation of a `WaitGroup`.
//!
//! A `WaitGroup` waits for a collection of tasks to finish. The main task can create new workers and
//! pass them to each of the tasks it wants to wait for. Then, each of the tasks calls `done` when
//! it finishes executing. The main task can call `wait` to block until all registered workers are done.
//!
//! # Examples
//!
//! ```rust
//! # fn main() {
//! # let rt = tokio::runtime::Builder::new_current_thread().enable_time().enable_io().build().unwrap();
//! # rt.block_on(async {
//! use awaitgroup::WaitGroup;
//!
//! let mut wg = WaitGroup::new();
//!
//! for _ in 0..5 {
//!  let wg = wg.clone();
//!     // Create a new worker.
//!     wg.add();
//!
//!     tokio::spawn(async move {
//!         // Do some work...
//!
//!         // This task is done all of its work.
//!         wg.done();
//!     });
//! }
//!
//! // Block until all other tasks have finished their work.
//! wg.wait().await;
//! # });
//! # }
//! ```
//!
//! A `WaitGroup` can be re-used and awaited multiple times.
//! ```rust
//! # use awaitgroup::WaitGroup;
//! # fn main() {
//! # let rt = tokio::runtime::Builder::new_current_thread().enable_time().enable_io().build().unwrap();
//! # rt.block_on(async {
//! let mut wg = WaitGroup::new();
//!
//! let wgg = wg.guard_add();
//!
//! tokio::spawn(async move {
//!     // Do work...
//!     let _wgg = wgg;
//! });
//!
//! // Wait for tasks to finish
//! wg.wait().await;
//!
//! // Re-use wait group
//! let wgg = wg.guard_add();
//!
//! tokio::spawn(async move {
//!     // Do more work...
//!    let _wgg = wgg;
//! });
//!
//! wg.wait().await;
//! # });
//! # }
//! ```
//!
//! If a previous round ended with an error, call [`WaitGroup::reset_error`]
//! before reusing the group — otherwise the next `wait` will immediately
//! return the stale error.
//! ```rust
//! # use awaitgroup::WaitGroup;
//! # use anyhow::anyhow;
//! # fn main() {
//! # let rt = tokio::runtime::Builder::new_current_thread().enable_time().enable_io().build().unwrap();
//! # rt.block_on(async {
//! let wg = WaitGroup::new();
//!
//! {
//!     let wg = wg.clone();
//!     wg.add();
//!     tokio::spawn(async move {
//!         wg.set_error(anyhow!("something went wrong"));
//!     });
//! }
//!
//! assert!(wg.wait().await.is_err());
//!
//! // Clear the sticky error before reusing.
//! wg.reset_error();
//!
//! let wgg = wg.guard_add();
//! tokio::spawn(async move {
//!     let _wgg = wgg;
//! });
//!
//! assert!(wg.wait().await.is_ok());
//! # });
//! # }
//! ```
#![deny(missing_debug_implementations, rust_2018_idioms)]
use anyhow::anyhow;
use anyhow::Result;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner {
    waker: Mutex<Option<Waker>>,
    count: AtomicI32,
    error: Mutex<Option<Arc<anyhow::Error>>>,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            waker: Mutex::new(None),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
        }
    }

    pub fn set_waker(&self, waker: Waker) {
        *self.waker.lock().unwrap() = Some(waker);
    }

    pub fn notify(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    pub fn set_error(&self, err: anyhow::Error) {
        {
            *self.error.lock().unwrap() = Some(Arc::new(err));
        }

        self.notify();
    }

    pub fn reset_error(&self) {
        *self.error.lock().unwrap() = None;
    }

    pub fn get_error(&self) -> Option<Arc<anyhow::Error>> {
        self.error.lock().unwrap().clone()
    }

    pub fn done(&self) {
        let count = self.count.fetch_sub(1, Ordering::SeqCst);
        if count <= 0 {
            panic!("WaitGroup count < 0");
        }
        // We are the last worker
        if count == 1 {
            self.notify();
        }
    }

    pub fn count(&self) -> i32 {
        self.count.load(Ordering::SeqCst)
    }
}

/// Wait for a collection of tasks to finish execution.
///
/// Refer to the [crate level documentation](crate) for examples.
///
/// # Invariants (caller's responsibility)
///
/// The library performs no bookkeeping beyond a single atomic counter and a
/// single waker slot. The caller is responsible for upholding the following
/// invariants — violating them leads to panics, deadlocks, or lost errors:
///
/// 1. **Balanced add/done.** Every [`add`](Self::add) / [`add_num`](Self::add_num)
///    / [`guard_add`](Self::guard_add) must be balanced by **exactly one**
///    matching [`done`](Self::done) / [`set_error`](Self::set_error) /
///    [`WaitGroupGuard`] drop. Calling `done` (or `set_error`) more times than
///    `add` will panic; calling fewer times will make [`wait`](Self::wait)
///    hang forever.
/// 2. **Never `done` and `set_error` for the same worker.**
///    [`set_error`](Self::set_error) internally performs one `done`, so each
///    worker must call **either** `done` **or** `set_error`, not both.
/// 3. **Single waiter.** At most one task may be in [`wait`](Self::wait) at a
///    time. Concurrent waiters are **not** supported — only the most recently
///    registered waker is kept, so earlier waiters may be stuck forever.
/// 4. **Reset before reuse after error.** Once any worker calls `set_error`,
///    the error is sticky and every subsequent `wait` returns `Err`. Call
///    [`reset_error`](Self::reset_error) before reusing the `WaitGroup` for a
///    new round of work.
#[derive(Clone)]
pub struct WaitGroup {
    inner: Arc<Inner>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::new()),
        }
    }
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("WaitGroup").field("count", &count).finish()
    }
}

#[allow(clippy::new_without_default)]
impl WaitGroup {
    /// Creates a new `WaitGroup` with an initial count of `0`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically increments the worker count by `1` and returns a
    /// [`WaitGroupGuard`] that decrements the count back when dropped.
    ///
    /// Prefer this over the manual [`add`](Self::add) + [`done`](Self::done)
    /// pair when you want RAII semantics (e.g. so a panicking or cancelled
    /// task still releases its slot).
    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();
        WaitGroupGuard {
            inner: self.inner.clone(),
        }
    }

    /// Increments the worker count by `1`.
    ///
    /// Must be paired with **exactly one** later call to [`done`](Self::done)
    /// or [`set_error`](Self::set_error). See the
    /// [type-level invariants](Self#invariants-callers-responsibility).
    pub fn add(&self) {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
    }

    /// Increments the worker count by `num`.
    ///
    /// Must be paired with exactly `num` later calls to [`done`](Self::done)
    /// (or [`set_error`](Self::set_error)). The caller is responsible for
    /// keeping the running count non-negative; the library does not validate
    /// `num`.
    pub fn add_num(&self, num: i32) {
        self.inner.count.fetch_add(num, Ordering::SeqCst);
    }

    /// Decrements the worker count by `1` and notifies the current waiter if
    /// the count reaches `0`.
    ///
    /// # Panics
    ///
    /// Panics if the count is already `0` (i.e. `done` has been called more
    /// times than `add`). See the
    /// [type-level invariants](Self#invariants-callers-responsibility).
    pub fn done(&self) {
        self.inner.done()
    }

    /// Waits until either the worker count reaches `0` or a worker reports an
    /// error via [`set_error`](Self::set_error).
    ///
    /// # Returns
    ///
    /// - `Ok(())` once all registered workers have called `done` and no error
    ///   has been recorded.
    /// - `Err(_)` if any worker called `set_error`. The error is **sticky**;
    ///   call [`reset_error`](Self::reset_error) before reusing this
    ///   `WaitGroup` for a new round of work.
    ///
    /// # Concurrency
    ///
    /// Only **one** task may be inside `wait` at a time. Concurrent waiters
    /// are not supported — only the most recently registered waker is kept,
    /// so earlier waiters can be stuck forever. See the
    /// [type-level invariants](Self#invariants-callers-responsibility).
    pub async fn wait(&self) -> Result<()> {
        WaitGroupFuture::new(&self.inner).await
    }

    /// Returns the current worker count.
    ///
    /// This is informational and inherently racy — by the time the caller
    /// inspects the value, other threads may have changed it.
    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    /// Records an error and decrements the worker count by `1` (acts as a
    /// failing [`done`](Self::done)).
    ///
    /// A worker must call **either** `done` **or** `set_error`, never both —
    /// `set_error` already performs one `done` internally. Calling both will
    /// over-decrement the count and eventually panic.
    ///
    /// The error is sticky: once set, every subsequent [`wait`](Self::wait)
    /// returns `Err` until [`reset_error`](Self::reset_error) is called.
    ///
    /// # Panics
    ///
    /// Panics if the count is already `0` (same condition as `done`).
    pub fn set_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.done();
    }

    /// Clears any error previously recorded by [`set_error`](Self::set_error).
    ///
    /// Call this before reusing the `WaitGroup` after a failed `wait`,
    /// otherwise the next `wait` will immediately return the stale error.
    /// It is safe to call when no error is set (it is a no-op in that case).
    pub fn reset_error(&self) {
        self.inner.reset_error();
    }
}

/// RAII handle returned by [`WaitGroup::guard_add`].
///
/// Dropping the guard decrements the underlying worker count by `1`,
/// equivalent to calling [`WaitGroup::done`]. This makes it convenient to
/// pair an `add` with its matching `done` even in the presence of early
/// returns, cancellations, or panics.
///
/// # Panics
///
/// `Drop` calls `done` internally, which panics if the count is already `0`.
/// This should never happen under normal use (each guard owns exactly one
/// slot) unless the caller has manually called `done`/`set_error` on the
/// `WaitGroup` for this guard's slot.
pub struct WaitGroupGuard {
    inner: Arc<Inner>,
}

impl Drop for WaitGroupGuard {
    fn drop(&mut self) {
        self.inner.done()
    }
}

impl fmt::Debug for WaitGroupGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("WaitGroupGuard")
            .field("count", &count)
            .finish()
    }
}

impl WaitGroupGuard {
    /// Records an error on the underlying `WaitGroup` **without** decrementing
    /// the worker count — the matching decrement happens when this guard is
    /// dropped.
    ///
    /// This is intentionally different from [`WaitGroup::set_error`], which
    /// acts as a failing `done`. Because the guard already owns the matching
    /// `done` via its `Drop` impl, this method only writes the error.
    ///
    /// The error is sticky; call [`WaitGroup::reset_error`] before reusing
    /// the `WaitGroup` after a failed `wait`.
    pub fn set_error(&self, err: anyhow::Error) {
        self.inner.set_error(err)
    }
}

struct WaitGroupFuture<'a> {
    inner: &'a Arc<Inner>,
}

impl<'a> WaitGroupFuture<'a> {
    fn new(inner: &'a Arc<Inner>) -> Self {
        Self { inner }
    }
}

impl Future for WaitGroupFuture<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.set_waker(cx.waker().clone());
        let count = self.inner.count();
        if count < 0 {
            return Poll::Ready(Err(anyhow!("err:count < 0 => count:{}", count)));
        }

        if let Some(e) = self.inner.get_error() {
            return Poll::Ready(Err(anyhow!(
                "err:error => count:{}, err:{}",
                self.inner.count(),
                e
            )));
        }

        match count {
            0 => Poll::Ready(Ok(())),
            _ => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_wait_group() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(async move {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                let wg = wg.clone();
                wg.add();

                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    wg.done();
                });
            }

            let ret = wg.wait().await;
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_wait_error() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(async move {
            let wg = WaitGroup::new();
            for i in 0..5 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if i == 3 {
                        wg.set_error(anyhow!("error: i == 3"));
                    } else {
                        wg.done();
                    }
                });
            }

            let ret = wg.wait().await;
            assert!(ret.is_err());
        });
    }

    #[test]
    fn test_wait_group_reuse() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                let wgg = wg.guard_add();
                tokio::spawn(async move {
                    let _wgg = wgg;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                });
            }

            let ret = wg.wait().await;
            assert!(ret.is_ok());

            let wgg = wg.guard_add();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                drop(wgg);
            });

            let ret = wg.wait().await;
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_worker_clone() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    let wgg = wg.guard_add();
                    tokio::spawn(async move {
                        let _wgg = wgg;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    });
                    wg.done();
                });
            }

            let ret = wg.wait().await;
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_worker_clone_error() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();

        rt.block_on(async {
            let wg = WaitGroup::new();
            for i in 0..5 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    let wgg = wg.guard_add();
                    tokio::spawn(async move {
                        let wgg = wgg;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if i == 3 {
                            wgg.set_error(anyhow!("error: i == 3"));
                        }
                    });
                    wg.done();
                });
            }

            let ret = wg.wait().await;
            assert!(ret.is_err());
        });
    }
}
