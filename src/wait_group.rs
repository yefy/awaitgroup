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
}

impl Inner {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
            is_waiting: AtomicBool::new(false),
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
        if self.is_waiting.load(Ordering::SeqCst) {
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

        WaitGroupFuture::new(self.inner.clone()).await
    }

    /// compatibility
    pub fn worker(&self) -> WaitGroupWorker {
        WaitGroupWorker {
            inner: self.inner.clone(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TASK_DELAY: Duration = Duration::from_millis(20);
    const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    async fn assert_wait_ok(wg: &WaitGroup) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, wg.wait()).await;
        assert!(ret.is_ok(), "wait timed out");
        assert!(ret.unwrap().is_ok(), "wait returned error");
    }

    async fn assert_wait_err(wg: &WaitGroup) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, wg.wait()).await;
        assert!(ret.is_ok(), "wait timed out");
        let err = ret.unwrap().unwrap_err();
        assert!(
            format!("{}", err).contains("err:error"),
            "unexpected error message: {}",
            err
        );
    }

    #[test]
    fn test_wait_immediate_when_count_zero() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            assert_eq!(wg.count(), 0);
            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_wait_group() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();

            for _ in 0..5 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }

            assert_wait_ok(&wg).await;
            assert_eq!(wg.count(), 0);
        });
    }

    #[test]
    fn test_add_num() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add_num(5);

            for _ in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }

            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_wait_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for i in 0..5 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    if i == 3 {
                        wg.done_error(anyhow!("error: i == 3"));
                    } else {
                        wg.done();
                    }
                });
            }

            assert_wait_err(&wg).await;
        });
    }

    #[test]
    fn test_first_error_is_kept() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.done_error(anyhow!("first"));
            wg.done_error(anyhow!("second"));

            let err = wg.wait().await.unwrap_err();
            assert!(format!("{}", err).contains("first"));
            assert!(!format!("{}", err).contains("second"));
        });
    }

    #[test]
    fn test_guard_drop_and_double_done() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            let guard = wg.guard_add();
            assert_eq!(wg.count(), 1);

            guard.done();
            assert_eq!(wg.count(), 0);

            guard.done();
            assert_eq!(wg.count(), 0, "second done must be idempotent");

            drop(wg.guard_add());
            assert_eq!(wg.count(), 0, "drop must decrement exactly once");
        });
    }

    #[test]
    fn test_wait_group_guard() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            // add / guard_add must finish before wait(); add during wait panics.
            for _ in 0..5 {
                let wgg = wg.guard_add();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    drop(wgg);
                });
            }
            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_wait_returns_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.done_error(anyhow!("sticky error"));
            assert_wait_err(&wg).await;
        });
    }

    #[test]
    fn test_worker_clone() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                let wg = wg.clone();
                wg.add();
                let wgg = wg.guard_add();
                tokio::spawn(async move {
                    tokio::spawn(async move {
                        tokio::time::sleep(TASK_DELAY).await;
                        drop(wgg);
                    });
                    wg.done();
                });
            }
            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_worker_clone_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for i in 0..5 {
                let wg = wg.clone();
                wg.add();
                let wgg = wg.guard_add();
                tokio::spawn(async move {
                    tokio::spawn(async move {
                        tokio::time::sleep(TASK_DELAY).await;
                        if i == 3 {
                            wgg.done_error(anyhow!("error: i == 3"));
                        }
                    });
                    wg.done();
                });
            }
            assert_wait_err(&wg).await;
        });
    }

    #[test]
    #[should_panic(expected = "Other threads might still be using it")]
    fn test_concurrent_wait_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            tokio::spawn(async move {
                wg2.wait().await.unwrap();
            });

            tokio::task::yield_now().await;
            let _ = wg.wait().await;
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
                    tokio::time::sleep(TASK_DELAY).await;
                    wi.done();
                });
            }

            assert_wait_ok(&wg).await;
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
                let nested_wgi = wgi.worker().add();
                tokio::spawn(async move {
                    tokio::spawn(async move {
                        nested_wgi.done();
                    });
                    tokio::time::sleep(TASK_DELAY).await;
                    wgi.done();
                });
            }

            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_inner_double_done() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            let wi = wg.worker().add();
            wi.done();
            wi.done();
            assert_eq!(wg.count(), 0);
            assert_wait_ok(&wg).await;
        });
    }
}
