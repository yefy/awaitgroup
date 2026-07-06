#[cfg(test)]
pub mod wait_group_test;

use anyhow::{anyhow, Result};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct Inner {
    notify: Notify,

    count: AtomicI32,

    version: AtomicU64,

    error: Mutex<Option<Arc<anyhow::Error>>>,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),

            count: AtomicI32::new(0),

            version: AtomicU64::new(0),

            error: Mutex::new(None),
        }
    }

    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    #[inline]
    pub fn notify_all(&self) {
        self.version.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub fn get_error(&self) -> Option<Arc<anyhow::Error>> {
        self.error.lock().unwrap().clone()
    }

    pub fn add(&self) {
        self.add_num(1);
    }

    pub fn add_num(&self, num: usize) {
        self.count.fetch_add(num as i32, Ordering::SeqCst);
    }

    pub fn done(&self) {
        let prev_count = self.count.fetch_sub(1, Ordering::SeqCst);

        let count = prev_count - 1;

        if count < 0 {
            panic!("WaitGroup count < 0");
        }

        //
        // count变化后必须通知
        //
        self.notify_all();
    }

    pub fn count(&self) -> i32 {
        self.count.load(Ordering::SeqCst)
    }

    pub fn set_error(&self, err: anyhow::Error) {
        let mut error = self.error.lock().unwrap();

        if error.is_none() {
            *error = Some(Arc::new(err));
        }

        drop(error);

        //
        // error变化后必须通知
        //
        self.notify_all();
    }
}

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
        f.debug_struct("WaitGroup")
            .field("count", &self.count())
            .finish()
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self) {
        self.inner.add();
    }

    pub fn add_num(&self, num: usize) {
        self.inner.add_num(num);
    }

    pub fn done(&self) {
        self.inner.done();
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);

        self.inner.done();
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();

        WaitGroupGuard::new(self.inner.clone())
    }

    pub async fn wait(&self) -> Result<()> {
        let mut version = self.inner.version();

        loop {
            let count = self.inner.count();

            if count < 0 {
                panic!("WaitGroup count < 0");
            }

            if let Some(err) = self.inner.get_error() {
                return Err(anyhow!("err:error => count:{}, err:{}", count, err));
            }

            if count == 0 {
                return Ok(());
            }

            //
            // 先注册 waiter
            //
            let notified = self.inner.notify.notified();

            //
            // 再检查 version
            //
            let new_version = self.inner.version();

            if new_version != version {
                version = new_version;
                continue;
            }

            //
            // 真正等待
            //
            notified.await;

            //
            // 醒来后更新 version
            //
            version = self.inner.version();
        }
    }

    /// compatibility
    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker::new(self.inner.clone())
    }
}

pub struct WaitGroupGuard {
    inner: Arc<Inner>,

    is_done: AtomicBool,
}

impl fmt::Debug for WaitGroupGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitGroupGuard")
            .field("count", &self.inner.count())
            .finish()
    }
}

impl Drop for WaitGroupGuard {
    fn drop(&mut self) {
        self.done();
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
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn done(&self) {
        if self.lock_done() {
            self.inner.done();
        }
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);

        self.done();
    }
}

#[derive(Clone)]
pub struct WaitGroupWorker {
    inner: Arc<Inner>,
}

impl fmt::Debug for WaitGroupWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("count", &self.inner.count())
            .finish()
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
}

#[derive(Clone)]
pub struct WaitGroupInner {
    inner: Arc<Inner>,

    is_done: Arc<AtomicBool>,
}

impl fmt::Debug for WaitGroupInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerInner")
            .field("count", &self.inner.count())
            .finish()
    }
}

impl WaitGroupInner {
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,

            is_done: Arc::new(AtomicBool::new(false)),
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
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn done(&self) {
        if self.lock_done() {
            self.inner.done();
        }
    }

    pub fn try_done_error(&self, err: anyhow::Error) {
        if self.lock_done() {
            self.inner.set_error(err);

            self.inner.done();
        }
    }

    pub fn done_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);

        self.done();
    }
}
