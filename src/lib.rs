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
    wait_arrive_count: AtomicI32,
    is_waiting: Arc<AtomicBool>,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            waker: Mutex::new(None),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
            wait_arrive_count: AtomicI32::new(0),
            is_waiting: Arc::new(AtomicBool::new(false)),
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

    pub fn reset(&self) {
        let is_waiting = self.is_waiting.load(Ordering::SeqCst);
        if is_waiting {
            panic!("Other threads might still be using it")
        }
        self.is_waiting.store(false, Ordering::SeqCst);
        *self.error.lock().unwrap() = None;
        let wait_arrive_count = self.wait_arrive_count.swap(0, Ordering::SeqCst);
        let count = self.count.swap(0, Ordering::SeqCst);
        if wait_arrive_count > 0 {
            if count != wait_arrive_count {
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
        let wait_arrive_count = self.wait_arrive_count();
        if wait_arrive_count > 0 {
            if count > wait_arrive_count {
                panic!("Other threads might still be using it")
            } else if count == wait_arrive_count {
                self.notify();
            }
        }
    }

    pub fn add_num(&self, num: usize) {
        let count = self.count.fetch_add(num as i32, Ordering::SeqCst) + num as i32;
        let wait_arrive_count = self.wait_arrive_count();
        if wait_arrive_count > 0 {
            if count > wait_arrive_count {
                panic!("Other threads might still be using it")
            } else if count == wait_arrive_count {
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

    pub fn wait_arrive_count(&self) -> i32 {
        self.wait_arrive_count.load(Ordering::SeqCst)
    }

    pub fn wait_arrive_count_swap(&self, value: i32) -> i32 {
        self.wait_arrive_count.swap(value, Ordering::SeqCst)
    }

    fn lock_waiting(&self) -> bool {
        self.is_waiting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn set_error(&self, err: anyhow::Error) {
        let mut error = self.error.lock().unwrap();
        if error.is_none() {
            *error = Some(Arc::new(err));
        }
    }
}

//wait wait_arrive reset 不能多线程使用, 不要在考虑多线程问题了, 没完没了的, 只能在一个线程用, 用户自己保证
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();
        WaitGroupGuard::new(self.inner.clone())
    }

    pub fn add(&self) {
        self.inner.add();
    }

    pub fn add_num(&self, num: usize) {
        self.inner.add_num(num)
    }

    pub fn done(&self) {
        self.inner.done()
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.done();
        self.inner.notify();
    }

    //等待结束使用: add add_num done done_error wait
    pub async fn wait(&self) -> Result<()> {
        let is_waiting = self.inner.is_waiting.clone();
        scopeguard::defer! {
            is_waiting.store(false, Ordering::SeqCst);
        };
        if !self.inner.lock_waiting() {
            panic!("Other threads might still be using it")
        }
        let wait_arrive_count = self.inner.wait_arrive_count_swap(0);
        if wait_arrive_count != 0 {
            panic!("Other threads might still be using it.")
        }
        WaitGroupFuture::new(&self.inner).await
    }

    pub fn reset(&self) {
        self.inner.reset();
    }

    ///下面的接口是为了兼容老版本
    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker {
            inner: self.inner.clone(),
        }
    }

    pub fn arrive(&self) {
        self.inner.add();
    }

    pub fn arrive_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.add();
        self.inner.notify();
    }

    //等待完成使用: arrive arrive_error wait_arrive
    pub async fn wait_arrive(&self, count: usize) -> Result<()> {
        let is_waiting = self.inner.is_waiting.clone();
        scopeguard::defer! {
            is_waiting.store(false, Ordering::SeqCst);
        };

        if !self.inner.lock_waiting() {
            panic!("Other threads might still be using it")
        }

        let wait_arrive_count = self.inner.wait_arrive_count_swap(count as i32);
        if wait_arrive_count > 0 && wait_arrive_count != count as i32 {
            panic!("Other threads might still be using it.")
        }
        WaitGroupFuture::new(&self.inner).await
    }
}

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
        self.inner.notify();
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
        let wait_arrive_count = self.inner.wait_arrive_count();
        if wait_arrive_count > 0 && count > wait_arrive_count {
            return Poll::Ready(Err(anyhow!(
                "err:count:{} > wait_arrive_count:{}",
                count,
                wait_arrive_count
            )));
        }

        if let Some(e) = self.inner.get_error() {
            return Poll::Ready(Err(anyhow!(
                "err:error => count:{}, err:{}",
                self.inner.count(),
                e
            )));
        }

        if count == wait_arrive_count {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

///下面的接口是为了兼容老版本
#[derive(Clone)]
pub struct WaitGroupWorker {
    inner: Arc<Inner>,
}

impl fmt::Debug for WaitGroupWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("Worker").field("count", &count).finish()
    }
}

impl WaitGroupWorker {
    fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    pub fn worker(&self) -> Self {
        Self::new(self.inner.clone())
    }

    pub fn add(&self) -> WaitGroupInner {
        self.inner.add();
        WaitGroupInner::new(self.inner.clone())
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.inner.add();
        WaitGroupGuard::new(self.inner.clone())
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn arrive(&self) {
        self.inner.add();
    }

    pub fn arrive_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.add();
        self.inner.notify();
    }
}

///下面的接口是为了兼容老版本
pub struct WaitGroupInner {
    inner: Arc<Inner>,
    is_done: AtomicBool,
}

impl fmt::Debug for WaitGroupInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.inner.count();
        f.debug_struct("WorkerInner")
            .field("count", &count)
            .finish()
    }
}

impl WaitGroupInner {
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            is_done: AtomicBool::new(false),
        }
    }

    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker::new(self.inner.clone())
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
        self.inner.notify();
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

            wg.reset();
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

            wg.reset();
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

            wg.reset();
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

            wg.reset();
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

            wg.reset();
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
                    wg.arrive();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 5);

            wg.reset();
        });
    }

    #[test]
    #[should_panic]
    fn test_wait_complete_error() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for i in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    if i == 3 {
                        wg.done_error(anyhow!("error: i == 3"));
                    } else {
                        wg.add();
                    }
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());

            wg.reset();
        });
    }

    #[test]
    fn test_wait_complete_ok() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();

            for i in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    if i == 3 {
                        wg.arrive_error(anyhow!("error: i == 3"));
                    } else {
                        wg.add();
                    }
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());

            wg.reset();
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
                    let _inner = worker.arrive();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(worker.count(), 5);

            wg.reset();
        });
    }

    #[test]
    #[should_panic]
    fn test_worker_wait_complete_error() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let worker = wg.worker();

            for i in 0..5 {
                let worker = worker.worker();
                tokio::spawn(async move {
                    if i == 3 {
                        worker.add().done_error(anyhow!("error: i == 3"));
                    } else {
                        worker.arrive()
                    }
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_err());

            wg.reset();
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

            wg.reset();
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
                    wg.arrive();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(3)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 3);
            wg.reset();

            for _ in 0..3 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    wg.arrive();
                });
            }

            let ret =
                tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait_arrive(3)).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            assert_eq!(wg.count(), 3);

            wg.reset();
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

            wg.reset();
        });
    }

    #[test]
    fn test_wait_group_worker_reuse() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let wgw = wg.worker();

            for _ in 0..5 {
                let wgi = wgw.add();

                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    wgi.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());
            wg.reset();

            let wgw = wg.worker();
            let wgi = wgw.add();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                wgi.done();
            });

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            wg.reset();
        });
    }

    #[test]
    fn test_wait_group_worker_clone() {
        let rt = current_thread_runtime();

        rt.block_on(async {
            let wg = WaitGroup::new();
            let wgw = wg.worker();

            for _ in 0..5 {
                let wgi = wgw.worker().add();

                tokio::spawn(async move {
                    let nested_wgi = wgi.worker().add();
                    tokio::spawn(async move {
                        nested_wgi.done();
                    });
                    wgi.done();
                });
            }

            let ret = tokio::time::timeout(tokio::time::Duration::from_secs(10), wg.wait()).await;
            assert!(ret.is_ok());
            let ret = ret.unwrap();
            assert!(ret.is_ok());

            wg.reset();
        });
    }
}
