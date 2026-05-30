use anyhow::{anyhow, Result};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner {
    wakers: Mutex<Vec<Waker>>,
    count: AtomicI32,
    error: Mutex<Option<Arc<anyhow::Error>>>,
    wait_arrive_count: AtomicI32,
}

impl Inner {
    pub fn new() -> Self {
        Self {
            wakers: Mutex::new(Vec::new()),
            count: AtomicI32::new(0),
            error: Mutex::new(None),
            wait_arrive_count: AtomicI32::new(0),
        }
    }

    pub fn notify(&self) {
        let mut wakers = self.wakers.lock().unwrap();

        for w in wakers.drain(..) {
            w.wake();
        }
    }

    pub fn register(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock().unwrap();

        if !wakers.iter().any(|w| w.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    pub fn get_error(&self) -> Option<Arc<anyhow::Error>> {
        self.error.lock().unwrap().clone()
    }

    pub fn add_num(&self, num: usize) {
        let prev_count = self.count.fetch_add(num as i32, Ordering::SeqCst);

        let count = prev_count + num as i32;

        let wait_arrive_count = self.wait_arrive_count();

        if wait_arrive_count > 0 {
            if count > wait_arrive_count {
                panic!("Other threads might still be using it");
            }

            if count == wait_arrive_count {
                self.notify();
            }
        }
    }

    pub fn add(&self) {
        self.add_num(1);
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

    pub fn set_error(&self, err: anyhow::Error) {
        let mut error = self.error.lock().unwrap();

        if error.is_none() {
            *error = Some(Arc::new(err));
        }

        drop(error);
    }
}

#[derive(Clone)]
pub struct WaitGroupArrive {
    inner: Arc<Inner>,
}

impl Default for WaitGroupArrive {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::new()),
        }
    }
}

impl fmt::Debug for WaitGroupArrive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitGroupArrive")
            .field("count", &self.count())
            .finish()
    }
}

impl WaitGroupArrive {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    pub async fn wait_arrive(&self, count: usize) -> Result<()> {
        if count == 0 {
            panic!("wait_arrive count <= 0");
        }

        let old = self.inner.wait_arrive_count_swap(count as i32);

        if old > 0 && old != count as i32 {
            panic!("wait_arrive count not same");
        }

        WaitGroupFuture::new(self.inner.clone()).await
    }

    pub fn worker(&self) -> WaitGroupWorkerArrive {
        WaitGroupWorkerArrive::new(self.inner.clone())
    }

    pub fn arrive(&self) {
        self.inner.add();
    }

    pub fn arrive_num(&self, num: usize) {
        self.inner.add_num(num);
    }

    pub fn arrive_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);
        self.inner.add();
        self.inner.notify();
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
        // 1. first check
        //

        let count = self.inner.count();

        let wait_arrive_count = self.inner.wait_arrive_count();

        let error = self.inner.get_error();

        if count < 0 {
            if error.is_none() {
                return Poll::Ready(Err(anyhow!("err:count < 0 => count:{}", count)));
            }
        }

        if wait_arrive_count > 0 && count > wait_arrive_count {
            return Poll::Ready(Err(anyhow!(
                "err:count:{} > wait_arrive_count:{}",
                count,
                wait_arrive_count
            )));
        }

        if let Some(e) = error.clone() {
            return Poll::Ready(Err(anyhow!("err:error => count:{}, err:{}", count, e)));
        }

        if count == wait_arrive_count {
            return Poll::Ready(Ok(()));
        }

        //
        // 2. register
        //

        self.inner.register(cx.waker());

        //
        // 3. second check
        //

        let count = self.inner.count();

        let wait_arrive_count = self.inner.wait_arrive_count();

        let error = self.inner.get_error();

        if count < 0 {
            if error.is_none() {
                return Poll::Ready(Err(anyhow!("err:count < 0 => count:{}", count)));
            }
        }

        if wait_arrive_count > 0 && count > wait_arrive_count {
            return Poll::Ready(Err(anyhow!(
                "err:count:{} > wait_arrive_count:{}",
                count,
                wait_arrive_count
            )));
        }

        if let Some(e) = error {
            return Poll::Ready(Err(anyhow!("err:error => count:{}, err:{}", count, e)));
        }

        if count == wait_arrive_count {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone)]
pub struct WaitGroupWorkerArrive {
    inner: Arc<Inner>,
    is_add: Arc<AtomicBool>,
}

impl fmt::Debug for WaitGroupWorkerArrive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("count", &self.inner.count())
            .finish()
    }
}

impl WaitGroupWorkerArrive {
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            is_add: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn worker(&self) -> Self {
        Self::new(self.inner.clone())
    }

    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    fn lock_add(&self) -> bool {
        self.is_add
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn arrive(&self) {
        if self.lock_add() {
            self.inner.add();
        }
    }

    pub fn arrive_num(&self, num: usize) {
        if self.lock_add() {
            self.inner.add_num(num);
        }
    }

    pub fn try_arrive_error(&self, err: anyhow::Error) {
        if self.lock_add() {
            self.inner.set_error(err);
            self.inner.add();
            self.inner.notify();
        }
    }

    pub fn arrive_error(&self, err: anyhow::Error) {
        self.inner.set_error(err);

        if self.lock_add() {
            self.inner.add();
        }

        self.inner.notify();
    }
}
