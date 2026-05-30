use anyhow::anyhow;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::wait_group::WaitGroup;

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

    /// `unlock_waiting` runs when `wait` returns, so a later `wait` can take the lock again.
    #[test]
    fn test_second_wait_ok_after_unlock() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            assert_wait_ok(&wg).await;
            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    #[should_panic(expected = "WaitGroup::add called during wait")]
    fn test_add_during_wait_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move {
                wg2.wait().await.unwrap();
            });

            tokio::task::yield_now().await;
            wg.add();
            let _ = waiter.await;
        });
    }

    #[test]
    #[should_panic(expected = "WaitGroup::add called during wait")]
    fn test_add_after_wait_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            assert_wait_ok(&wg).await;
            wg.add();
        });
    }

    /// `done` is still allowed while `wait` is pending (only new `add` is blocked).
    #[test]
    fn test_done_during_wait_completes() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move {
                wg2.wait().await.unwrap();
            });

            tokio::task::yield_now().await;
            wg.done();
            assert!(waiter.await.is_ok());
        });
    }
}
