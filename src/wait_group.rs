use anyhow::{anyhow, Result};
use futures::task::AtomicWaker;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

struct Inner {
    waker: AtomicWaker,
    count: AtomicI32,
    error: Mutex<Option<Arc<anyhow::Error>>>,
    is_waiting: AtomicBool,
    is_closing: AtomicBool,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
            is_waiting: AtomicBool::new(false),
            is_closing: AtomicBool::new(false),
        }
    }

    pub fn notify(&self) {
        self.waker.wake();
    }

    pub fn get_error(&self) -> Option<Arc<anyhow::Error>> {
        self.error.lock().unwrap().clone()
    }

    pub fn add(&self) {
        self.add_num(1);
    }

    pub fn add_num(&self, num: usize) {
        if self.is_closing.load(Ordering::SeqCst) {
            panic!("WaitGroup::add called during wait");
        }
        self.count.fetch_add(num as i32, Ordering::SeqCst);
    }

    pub fn done(&self) {
        // 统一使用 SeqCst，配合 Future 中的双检查机制
        let prev_count = self.count.fetch_sub(1, Ordering::SeqCst);
        let count = prev_count - 1;

        if count < 0 {
            panic!("WaitGroup count < 0");
        }

        if count == 0 {
            self.notify();
        }
    }

    pub fn count(&self) -> i32 {
        self.count.load(Ordering::SeqCst)
    }

    pub fn lock_waiting(&self) -> bool {
        self.is_waiting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn unlock_waiting(&self) {
        self.is_waiting.store(false, Ordering::SeqCst);
    }

    pub fn set_error(&self, err: anyhow::Error) {
        let mut error = self.error.lock().unwrap();
        if error.is_none() {
            *error = Some(Arc::new(err));
        }
        drop(error);
        // 💡 标记错误后，外层会通过 done() 或者是明确的 notify 来驱使 Future 唤醒
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
        // 先设错误，后 done。即便 done 内部扣出负数也不会崩溃
        self.inner.done();
        // 额外强行唤醒一次，确保 wait 线程不用等 count 归零就能立即收到异常并返回
        self.inner.notify();
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub fn guard_add(&self) -> WaitGroupGuard {
        self.add();
        WaitGroupGuard::new(self.inner.clone())
    }

    pub async fn wait(&self) -> Result<()> {
        if !self.inner.lock_waiting() {
            panic!("Other threads might still be using it");
        }
        self.inner.is_closing.store(true, Ordering::SeqCst);
        scopeguard::defer! { self.inner.unlock_waiting() }

        WaitGroupFuture::new(self.inner.clone()).await
    }

    /// compatibility
    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker::new(self.inner.clone())
    }
}

pub struct WaitGroupFuture {
    inner: Arc<Inner>,
}

impl WaitGroupFuture {
    fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }
}

impl Future for WaitGroupFuture {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        //
        // 1. 第一次检查状态
        //
        let count = self.inner.count();
        if count < 0 {
            panic!("WaitGroup count < 0");
        }

        let error = self.inner.get_error();
        if let Some(e) = error {
            return Poll::Ready(Err(anyhow!("err:error => count:{}, err:{}", count, e)));
        }

        if count == 0 {
            return Poll::Ready(Ok(()));
        }

        //
        // 2. 注册 Waker
        //
        self.inner.waker.register(cx.waker());

        //
        // 3. 第二次检查（完美闭环 Wake-before-Pending 问题）
        //
        let count = self.inner.count();
        if count < 0 {
            panic!("WaitGroup count < 0");
        }

        let error = self.inner.get_error();
        if let Some(e) = error {
            return Poll::Ready(Err(anyhow!("err:error => count:{}, err:{}", count, e)));
        }

        if count == 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
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
        self.inner.notify();
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

pub struct WaitGroupInner {
    inner: Arc<Inner>,
    is_done: AtomicBool,
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
        self.inner.notify();
    }
}
