use anyhow::anyhow;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WaitGroup;
    use std::time::Duration;

    const TASK_DELAY: Duration = Duration::from_millis(20);
    const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    fn multi_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
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

    async fn assert_join_ok(handle: tokio::task::JoinHandle<anyhow::Result<()>>) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, handle).await;
        assert!(ret.is_ok(), "wait timed out");
        assert!(ret.unwrap().unwrap().is_ok(), "wait returned error");
    }

    async fn assert_join_err(handle: tokio::task::JoinHandle<anyhow::Result<()>>, msg: &str) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, handle).await;
        assert!(ret.is_ok(), "wait timed out");
        let err = ret.unwrap().unwrap().unwrap_err();
        assert!(
            format!("{}", err).contains(msg),
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

    /// Multiple concurrent `wait` calls on the same group must all complete when count reaches 0.
    #[test]
    fn test_concurrent_wait_same_group() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.add();

            let wg_a = wg.clone();
            let wg_b = wg.clone();
            let wg_c = wg.clone();

            let w1 = tokio::spawn(async move { wg_a.wait().await });
            let w2 = tokio::spawn(async move { wg_b.wait().await });
            let w3 = tokio::spawn(async move { wg_c.wait().await });

            tokio::task::yield_now().await;
            wg.done();
            wg.done();
            wg.done();

            assert!(w1.await.unwrap().is_ok());
            assert!(w2.await.unwrap().is_ok());
            assert!(w3.await.unwrap().is_ok());
        });
    }

    #[test]
    fn test_concurrent_wait_multi_thread_runtime() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                wg.add();
            }

            let mut handles = Vec::new();
            for _ in 0..3 {
                let wg = wg.clone();
                handles.push(tokio::spawn(async move { wg.wait().await }));
            }

            for _ in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }

            for h in handles {
                let ret = tokio::time::timeout(WAIT_TIMEOUT, h).await;
                assert!(ret.is_ok(), "wait timed out");
                assert!(ret.unwrap().unwrap().is_ok());
            }
        });
    }

    #[test]
    fn test_concurrent_wait_wakes_all_waiters() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let wg3 = wg.clone();

            let waiter1 = tokio::spawn(async move { wg2.wait().await });
            let waiter2 = tokio::spawn(async move { wg3.wait().await });

            tokio::task::yield_now().await;
            wg.done();

            assert!(waiter1.await.unwrap().is_ok());
            assert!(waiter2.await.unwrap().is_ok());
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

    /// Sequential `wait` on an idle group (count 0) is allowed after a prior `wait` completed.
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
    fn test_add_during_wait_extends_count() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;
            assert_eq!(wg.count(), 1);

            wg.add();
            assert_eq!(wg.count(), 2);

            wg.done();
            wg.done();

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(ret.unwrap().unwrap().is_ok());
        });
    }

    #[test]
    fn test_add_after_wait_completes() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            assert_wait_ok(&wg).await;

            wg.add();
            assert_eq!(wg.count(), 1);
            wg.done();
            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_add_num_during_wait() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;
            wg.add_num(2);
            assert_eq!(wg.count(), 3);

            wg.done();
            wg.done();
            wg.done();

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(ret.unwrap().unwrap().is_ok());
        });
    }

    #[test]
    fn test_worker_add_during_wait() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;

            let wi = wg.worker().add();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                wi.done();
            });

            wg.done();
            wg.done();

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(ret.unwrap().unwrap().is_ok());
        });
    }

    #[test]
    fn test_guard_add_during_wait() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;

            let guard = wg.guard_add();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                drop(guard);
            });

            wg.done();
            wg.done();

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(ret.unwrap().unwrap().is_ok());
        });
    }

    #[test]
    fn test_add_during_concurrent_wait_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let wg3 = wg.clone();
            let w1 = tokio::spawn(async move { wg2.wait().await });
            let w2 = tokio::spawn(async move { wg3.wait().await });

            tokio::task::yield_now().await;

            wg.add();
            let wg4 = wg.clone();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                wg4.done();
            });
            wg.done();
            wg.done();

            assert_join_ok(w1).await;
            assert_join_ok(w2).await;
        });
    }

    #[test]
    fn test_spawn_work_while_waiting() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;

            // Coordinator discovers more work while wait is pending.
            for _ in 0..3 {
                let wg = wg.clone();
                wg.add();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }
            wg.done();

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(ret.unwrap().unwrap().is_ok());
            assert_eq!(wg.count(), 0);
        });
    }

    /// All concurrent waiters observe the same sticky error.
    #[test]
    fn test_concurrent_wait_all_see_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.done_error(anyhow!("shared"));

            let wg2 = wg.clone();
            let wg3 = wg.clone();
            let w1 = tokio::spawn(async move { wg2.wait().await });
            let w2 = tokio::spawn(async move { wg3.wait().await });

            for h in [w1, w2] {
                let ret = h.await.unwrap();
                assert!(format!("{}", ret.unwrap_err()).contains("shared"));
            }
        });
    }

    /// `done` is allowed while `wait` is pending; so is `add`.
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

    /// Error set while count > 0 must wake pending waiters via Notify.
    #[test]
    fn test_error_wakes_waiter_before_count_zero() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();

            let wg2 = wg.clone();
            let waiter = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;
            wg.done_error(anyhow!("early failure"));

            let ret = tokio::time::timeout(WAIT_TIMEOUT, waiter).await;
            assert!(ret.is_ok(), "wait timed out");
            assert!(format!("{}", ret.unwrap().unwrap().unwrap_err()).contains("early failure"));
        });
    }

    #[test]
    fn test_try_done_error_is_idempotent() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            let wi = wg.worker().add();
            wi.try_done_error(anyhow!("once"));
            wi.try_done_error(anyhow!("ignored"));
            assert_eq!(wg.count(), 0);

            let err = wg.wait().await.unwrap_err();
            assert!(format!("{}", err).contains("once"));
            assert!(!format!("{}", err).contains("ignored"));
        });
    }

    /// Notify must not lose wakeups when `done` completes before `wait` registers.
    #[test]
    fn test_wait_after_all_done_returns_immediately() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.done();
            wg.done();

            assert_wait_ok(&wg).await;
        });
    }

    #[test]
    fn test_concurrent_wait_error_while_workers_running() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..5 {
                wg.add();
            }

            let wg2 = wg.clone();
            let wg3 = wg.clone();
            let w1 = tokio::spawn(async move { wg2.wait().await });
            let w2 = tokio::spawn(async move { wg3.wait().await });

            tokio::task::yield_now().await;
            wg.done_error(anyhow!("worker failure"));

            for h in [w1, w2] {
                let ret = tokio::time::timeout(WAIT_TIMEOUT, h).await;
                assert!(ret.is_ok(), "wait timed out");
                assert!(
                    format!("{}", ret.unwrap().unwrap().unwrap_err()).contains("worker failure")
                );
            }
        });
    }

    #[test]
    #[should_panic(expected = "WaitGroup count < 0")]
    fn test_done_too_many_times_panics() {
        let wg = WaitGroup::new();
        wg.add();
        wg.done();
        wg.done();
    }

    /// Many concurrent waiters on a multi-thread runtime must all complete.
    #[test]
    fn test_many_concurrent_waiters_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            const WAITERS: usize = 16;
            const WORK: usize = 16;

            let wg = WaitGroup::new();
            for _ in 0..WORK {
                wg.add();
            }

            let mut handles = Vec::with_capacity(WAITERS);
            for _ in 0..WAITERS {
                let wg = wg.clone();
                handles.push(tokio::spawn(async move { wg.wait().await }));
            }

            for _ in 0..WORK {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }

            for h in handles {
                assert_join_ok(h).await;
            }
        });
    }

    /// `done` from worker threads while multiple waiters are blocked.
    #[test]
    fn test_concurrent_wait_staggered_done_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.add();

            let wg_a = wg.clone();
            let wg_b = wg.clone();
            let wg_c = wg.clone();
            let w1 = tokio::spawn(async move { wg_a.wait().await });
            let w2 = tokio::spawn(async move { wg_b.wait().await });
            let w3 = tokio::spawn(async move { wg_c.wait().await });

            tokio::task::yield_now().await;

            let wg_done1 = wg.clone();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                wg_done1.done();
            });
            wg.done();
            let wg_done2 = wg.clone();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                wg_done2.done();
            });

            assert_join_ok(w1).await;
            assert_join_ok(w2).await;
            assert_join_ok(w3).await;
        });
    }

    /// Notify must wake every waiter when count reaches zero under thread contention.
    #[test]
    fn test_concurrent_wait_wakes_all_on_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();

            let mut handles = Vec::new();
            for _ in 0..8 {
                let wg = wg.clone();
                handles.push(tokio::spawn(async move { wg.wait().await }));
            }

            tokio::task::yield_now().await;
            wg.done();

            for h in handles {
                assert_join_ok(h).await;
            }
        });
    }

    /// Waiters started after partial `done` must still observe completion.
    #[test]
    fn test_late_waiter_joins_after_partial_done() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.add();

            wg.done();

            let wg2 = wg.clone();
            let early = tokio::spawn(async move { wg2.wait().await });

            tokio::task::yield_now().await;
            wg.done();
            wg.done();

            assert_join_ok(early).await;

            let wg3 = wg.clone();
            let late = tokio::spawn(async move { wg3.wait().await });
            assert_join_ok(late).await;
        });
    }

    /// Mixed success path: workers finish while several waiters are already pending.
    #[test]
    fn test_multi_thread_workers_with_pending_waiters() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            for _ in 0..10 {
                wg.add();
            }

            let mut waiters = Vec::new();
            for _ in 0..5 {
                let wg = wg.clone();
                waiters.push(tokio::spawn(async move { wg.wait().await }));
            }

            for _ in 0..10 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.done();
                });
            }

            for h in waiters {
                assert_join_ok(h).await;
            }
            assert_eq!(wg.count(), 0);
        });
    }

    /// All concurrent waiters must observe the same sticky error on multi-thread runtime.
    #[test]
    fn test_multi_thread_concurrent_wait_all_see_error() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();
            wg.done_error(anyhow!("mt-shared"));

            let mut handles = Vec::new();
            for _ in 0..6 {
                let wg = wg.clone();
                handles.push(tokio::spawn(async move { wg.wait().await }));
            }

            for h in handles {
                assert_join_err(h, "mt-shared").await;
            }
        });
    }

    /// `done` during concurrent wait from another thread must unblock all waiters.
    #[test]
    fn test_done_during_concurrent_wait_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();

            let wg2 = wg.clone();
            let wg3 = wg.clone();
            let w1 = tokio::spawn(async move { wg2.wait().await });
            let w2 = tokio::spawn(async move { wg3.wait().await });

            tokio::task::yield_now().await;

            let wg_done1 = wg.clone();
            tokio::spawn(async move {
                wg_done1.done();
            });
            wg.done();

            assert_join_ok(w1).await;
            assert_join_ok(w2).await;
        });
    }

    /// Sequential `wait` calls after workers finished must both succeed.
    #[test]
    fn test_sequential_wait_after_workers_done_multi_thread() {
        let rt = multi_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroup::new();
            wg.add();
            wg.add();

            let wg2 = wg.clone();
            tokio::spawn(async move {
                tokio::time::sleep(TASK_DELAY).await;
                wg2.done();
                wg2.done();
            });

            assert_wait_ok(&wg).await;
            assert_wait_ok(&wg).await;
        });
    }
}
