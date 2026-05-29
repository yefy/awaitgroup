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
//!         wg.done_error(anyhow!("something went wrong"));
//!     });
//! }
//!
//! assert!(wg.wait().await.is_err());
//!
//! // Clear the sticky error before reusing.
//! wg.reset();
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
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner {
    waker: Mutex<Option<Waker>>,
    count: AtomicI32,
    error: Mutex<Option<Arc<anyhow::Error>>>,
    wait_complete: AtomicI32,
    is_waiting: AtomicBool,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            waker: Mutex::new(None),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
            wait_complete: AtomicI32::new(0),
            is_waiting: AtomicBool::new(false),
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
            let mut error = self.error.lock().unwrap();
            if error.is_none() {
                *error = Some(Arc::new(err));
            }
        }

        self.notify();
    }

    pub fn reset(&self) {
        self.is_waiting.store(false, Ordering::SeqCst);
        *self.error.lock().unwrap() = None;
        let wait_complete = self.wait_complete.swap(0, Ordering::SeqCst);
        let count = self.count.swap(0, Ordering::SeqCst);
        if wait_complete > 0 {
            if count != wait_complete {
                panic!("Other threads might still be using it")
            }
        } else {
            if count != 0 {
                panic!("Other threads might still be using it")
            }
        }
    }

    pub fn get_error(&self) -> Option<Arc<anyhow::Error>> {
        self.error.lock().unwrap().clone()
    }

    pub fn add(&self) {
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        let wait_complete = self.wait_complete();
        if wait_complete > 0 {
            if count > wait_complete {
                panic!("Other threads might still be using it")
            } else if count == wait_complete {
                self.notify();
            }
        }
    }

    pub fn add_num(&self, num: usize) {
        let count = self.count.fetch_add(num as i32, Ordering::SeqCst) + num as i32;
        let wait_complete = self.wait_complete();
        if wait_complete > 0 {
            if count > wait_complete {
                panic!("Other threads might still be using it")
            } else if count == wait_complete {
                self.notify();
            }
        }
    }

    pub fn done(&self) {
        let count = self.count.fetch_sub(1, Ordering::SeqCst) - 1;
        if count < 0 {
            panic!("WaitGroup count < 0");
        }
        // We are the last worker
        if count == 0 {
            self.notify();
        }
    }

    pub fn count(&self) -> i32 {
        self.count.load(Ordering::SeqCst)
    }

    pub fn wait_complete(&self) -> i32 {
        self.wait_complete.load(Ordering::SeqCst)
    }

    pub fn wait_complete_swap(&self, value: i32) -> i32 {
        self.wait_complete.swap(value, Ordering::SeqCst)
    }

    fn lock_waiting(&self) -> bool {
        self.is_waiting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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
        WaitGroupGuard::new(self.inner.clone())
    }

    /// Increments the worker count by `1`.
    ///
    /// Must be paired with **exactly one** later call to [`done`](Self::done)
    /// or [`set_error`](Self::set_error). See the
    /// [type-level invariants](Self#invariants-callers-responsibility).
    pub fn add(&self) {
        self.inner.add();
    }

    /// Increments the worker count by `num`.
    ///
    /// Must be paired with exactly `num` later calls to [`done`](Self::done)
    /// (or [`set_error`](Self::set_error)). The caller is responsible for
    /// keeping the running count non-negative; the library does not validate
    /// `num`.
    pub fn add_num(&self, num: usize) {
        self.inner.add_num(num)
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

    /// Waits until the worker count reaches `0` (or an error is reported).
    ///
    /// Use with the **add → done** pattern: each worker calls [`add`](Self::add)
    /// (or [`guard_add`](Self::guard_add)) when it starts, and [`done`](Self::done)
    /// when it finishes. `wait` returns `Ok` once every `add` has a matching
    /// `done` and the count is back to zero.
    ///
    /// # Returns
    ///
    /// - `Ok(())` once all workers have called `done` and no error has been
    ///   recorded.
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
        if !self.inner.lock_waiting() {
            panic!("Other threads might still be using it")
        }
        let wait_complete = self.inner.wait_complete_swap(0);
        if wait_complete != 0 {
            panic!("Other threads might still be using it.")
        }
        let ret = WaitGroupFuture::new(&self.inner).await;
        self.inner.is_waiting.store(false, Ordering::SeqCst);
        ret
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
    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.done();
    }

    pub fn reset(&self) {
        self.inner.reset();
    }

    /// Legacy API: obtain a [`Worker`] handle for the add-only registration path.
    pub fn worker(&self) -> Worker {
        Worker {
            inner: self.inner.clone(),
        }
    }

    /// Legacy API: wait until `count` workers have called [`add`](Self::add).
    ///
    /// Unlike [`wait`](Self::wait), this does **not** require [`done`](Self::done).
    /// It returns once the worker count reaches `count`. Call [`reset`](Self::reset)
    /// before the next round when `count` still equals the `wait_complete`
    /// target from the last call.
    pub async fn wait_complete(&self, count: usize) -> Result<()> {
        if !self.inner.lock_waiting() {
            panic!("Other threads might still be using it")
        }

        let wait_complete = self.inner.wait_complete_swap(count as i32);
        if wait_complete > 0 && wait_complete != count as i32 {
            panic!("Other threads might still be using it.")
        }
        let ret = WaitGroupFuture::new(&self.inner).await;
        self.inner.is_waiting.store(false, Ordering::SeqCst);
        ret
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
    is_done: AtomicBool,
}

impl Drop for WaitGroupGuard {
    fn drop(&mut self) {
        self.done()
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
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            is_done: AtomicBool::new(false),
        }
    }

    fn lock_done(&self) -> bool {
        self.is_done
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn done(&self) {
        if self.lock_done() {
            self.inner.done()
        }
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.done();
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
        let wait_complete = self.inner.wait_complete();
        if wait_complete > 0 && count > wait_complete {
            return Poll::Ready(Err(anyhow!(
                "err:count:{} > wait_complete:{}",
                count,
                wait_complete
            )));
        }

        if let Some(e) = self.inner.get_error() {
            return Poll::Ready(Err(anyhow!(
                "err:error => count:{}, err:{}",
                self.inner.count(),
                e
            )));
        }

        if count == wait_complete {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

///下面的接口是为了兼容老版本
/// A worker registered in a `WaitGroup`.
///
/// Refer to the [crate level documentation](crate) for details.
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("Worker").field("count", &count).finish()
    }
}

impl Worker {
    /// Notify the `WaitGroup` that this worker has finished execution.
    pub fn worker(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
    pub fn add(&self) -> WorkerInner {
        self.inner.add();
        WorkerInner::new(self.inner.clone())
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();
        WaitGroupGuard::new(self.inner.clone())
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn set_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
    }
}

///下面的接口是为了兼容老版本
pub struct WorkerInner {
    inner: Arc<Inner>,
    is_done: AtomicBool,
}

impl fmt::Debug for WorkerInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("WorkerInner")
            .field("count", &count)
            .finish()
    }
}

impl WorkerInner {
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            is_done: AtomicBool::new(false),
        }
    }

    pub fn worker(&self) -> Worker {
        Worker {
            inner: self.inner.clone(),
        }
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    fn lock_done(&self) -> bool {
        self.is_done
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn done(&self) {
        if self.lock_done() {
            self.inner.done()
        }
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.done();
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

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
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
                        wg.done_error(anyhow!("error: i == 3"));
                    } else {
                        wg.done();
                    }
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
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

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            let wgg = wg.guard_add();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                drop(wgg);
            });

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
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

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
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
                            wgg.done_error(anyhow!("error: i == 3"));
                        }
                    });
                    wg.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());
        });
    }

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap()
    }

    #[test]
    fn test_wait_complete() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    wg.add();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(5))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 5);
        });
    }

    #[test]
    fn test_wait_complete_error() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for i in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    wg.add();
                    if i == 3 {
                        wg.done_error(anyhow!("error: i == 3"));
                    }
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(5))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());
        });
    }

    #[test]
    fn test_worker_wait_complete() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let worker = wg.worker();

            for _ in 0..5 {
                let worker = worker.worker();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _inner = worker.add();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(5))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(worker.count(), 5);
        });
    }

    #[test]
    fn test_worker_wait_complete_error() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let worker = wg.worker();

            for i in 0..5 {
                let worker = worker.worker();
                tokio::spawn(async move {
                    let inner = worker.add();
                    if i == 3 {
                        inner.done_error(anyhow!("error: i == 3"));
                    }
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(5))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());
        });
    }

    #[test]
    fn test_reset_after_error() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for i in 0..3 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if i == 1 {
                        wg.done_error(anyhow!("round 1 error"));
                    } else {
                        wg.done();
                    }
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());

            // All workers finished; count is 0 — safe to reset.
            tokio::time::sleep(Duration::from_millis(100)).await;
            wg.reset();

            for _ in 0..3 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    wg.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_reset_reuse_wait_complete() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..3 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    wg.add();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(3))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 3);
            wg.reset();

            for _ in 0..3 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    wg.add();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_complete(3))
                    .await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 3);
        });
    }

    #[test]
    fn test_wait_group_worker() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let wk = wg.worker();

            for _ in 0..5 {
                let wi = wk.add();

                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    wi.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_wait_group_worker_reuse() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let wk = wg.worker();

            for _ in 0..5 {
                let wi = wk.add();

                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    wi.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            let wk = wg.worker();
            let wi = wk.add();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                wi.done();
            });

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());
        });
    }

    #[test]
    fn test_wait_group_worker_clone() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let wk = wg.worker();

            for _ in 0..5 {
                let wi = wk.worker().add();

                tokio::spawn(async move {
                    let nested_wi = wi.worker().add();
                    tokio::spawn(async move {
                        nested_wi.done();
                    });
                    wi.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());
        });
    }
}
