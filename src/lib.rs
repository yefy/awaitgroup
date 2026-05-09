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
/// Refer to the [crate level documentation](crate) for details.
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
    /// Creates a new `WaitGroup`.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();
        WaitGroupGuard {
            inner: self.inner.clone(),
        }
    }

    pub fn add(&self) {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_num(&self, num: i32) {
        self.inner.count.fetch_add(num, Ordering::SeqCst);
    }

    pub fn done(&self) {
        self.inner.done()
    }

    /// Wait until all registered workers finish executing.
    pub async fn wait(&self) -> Result<()> {
        WaitGroupFuture::new(&self.inner).await
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn set_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.done();
    }
}

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
        if let Some(e) = self.inner.get_error() {
            return Poll::Ready(Err(anyhow!(
                "err:error => count:{}, err:{}",
                self.inner.count(),
                e
            )));
        }

        self.inner.set_waker(cx.waker().clone());
        let count = self.inner.count();
        if count < 0 {
            return Poll::Ready(Err(anyhow!("err:count < 0 => count:{}", count)));
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
