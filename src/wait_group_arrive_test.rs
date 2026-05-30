use anyhow::anyhow;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::wait_group_arrive::WaitGroupArrive;

    const TASK_DELAY: Duration = Duration::from_millis(20);
    const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    async fn assert_wait_arrive_ok(wg: &WaitGroupArrive, count: usize) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, wg.wait_arrive(count)).await;
        assert!(ret.is_ok(), "wait_arrive timed out");
        assert!(ret.unwrap().is_ok(), "wait_arrive returned error");
    }

    async fn assert_wait_arrive_err(wg: &WaitGroupArrive, count: usize) {
        let ret = tokio::time::timeout(WAIT_TIMEOUT, wg.wait_arrive(count)).await;
        assert!(ret.is_ok(), "wait_arrive timed out");
        let err = ret.unwrap().unwrap_err();
        assert!(
            format!("{}", err).contains("err:error"),
            "unexpected error message: {}",
            err
        );
    }

    #[test]
    fn test_wait_arrive_when_already_satisfied() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive();
            wg.arrive();
            assert_eq!(wg.count(), 2);

            assert_wait_arrive_ok(&wg, 2).await;
            assert_eq!(wg.count(), 2);
        });
    }

    #[test]
    fn test_wait_complete() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();

            for _ in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.arrive();
                });
            }

            assert_wait_arrive_ok(&wg, 5).await;
            assert_eq!(wg.count(), 5);
        });
    }

    #[test]
    fn test_arrive_num() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive_num(3);
            tokio::spawn({
                let wg = wg.clone();
                async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.arrive_num(2);
                }
            });

            assert_wait_arrive_ok(&wg, 5).await;
            assert_eq!(wg.count(), 5);
        });
    }

    #[test]
    fn test_wait_complete_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();

            for i in 0..5 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    if i == 3 {
                        wg.arrive_error(anyhow!("error: i == 3"));
                    } else {
                        wg.arrive();
                    }
                });
            }

            assert_wait_arrive_err(&wg, 5).await;
        });
    }

    #[test]
    fn test_first_error_is_kept() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive_error(anyhow!("first"));
            wg.arrive_error(anyhow!("second"));

            let err = wg.wait_arrive(2).await.unwrap_err();
            let msg = format!("{}", err);
            assert!(msg.contains("first"), "error message: {}", msg);
            assert!(!msg.contains("second"), "error message: {}", msg);
        });
    }

    #[test]
    fn test_exceed_wait_arrive_count() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();

            for _ in 0..6 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    wg.arrive();
                });
            }

            let ret = tokio::time::timeout(WAIT_TIMEOUT, wg.wait_arrive(5)).await;
            assert!(ret.is_ok());
            let err = ret.unwrap().unwrap_err();
            assert!(format!("{}", err).contains("count:6 > wait_arrive_count:5"));
        });
    }

    #[test]
    fn test_worker_wait_complete() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            let worker = wg.worker();

            for _ in 0..5 {
                let worker = worker.worker();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    worker.arrive();
                });
            }

            assert_wait_arrive_ok(&wg, 5).await;
            assert_eq!(worker.count(), 5);
        });
    }

    #[test]
    fn test_worker_wait_complete_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            let worker = wg.worker();

            for i in 0..5 {
                let worker = worker.worker();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    if i == 3 {
                        worker.arrive_error(anyhow!("error: i == 3"));
                    } else {
                        worker.arrive();
                    }
                });
            }

            assert_wait_arrive_err(&wg, 5).await;
        });
    }

    #[test]
    fn test_wait_arrive_returns_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive_error(anyhow!("sticky error"));
            assert_wait_arrive_err(&wg, 1).await;
        });
    }

    #[test]
    #[should_panic(expected = "wait_arrive count <= 0")]
    fn test_wait_arrive_zero_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            let _ = wg.wait_arrive(0).await;
        });
    }

    #[test]
    #[should_panic(expected = "Other threads might still be using it")]
    fn test_concurrent_wait_arrive_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();

            let wg2 = wg.clone();
            tokio::spawn(async move {
                wg2.wait_arrive(2).await.unwrap();
            });

            tokio::task::yield_now().await;
            let _ = wg.wait_arrive(2).await;
        });
    }

    #[test]
    #[should_panic(expected = "Other threads might still be using it")]
    fn test_arrive_exceeds_wait_arrive_count_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            let wg2 = wg.clone();

            let waiter = tokio::spawn(async move {
                wg2.wait_arrive(2).await.unwrap();
            });

            tokio::task::yield_now().await;
            wg.arrive();
            wg.arrive();
            wg.arrive();
            let _ = waiter.await;
        });
    }

    #[test]
    fn test_wait_arrive_same_target_twice() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();

            for _ in 0..3 {
                let wg = wg.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(TASK_DELAY).await;
                    wg.arrive();
                });
            }

            assert_wait_arrive_ok(&wg, 3).await;
            assert_eq!(wg.count(), 3);

            // Same target again: completes immediately when count is already satisfied.
            assert_wait_arrive_ok(&wg, 3).await;
            assert_eq!(wg.count(), 3);
        });
    }

    #[test]
    fn test_wait_arrive_same_target_after_error() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive_error(anyhow!("sticky error"));
            assert_wait_arrive_err(&wg, 1).await;
            assert_wait_arrive_err(&wg, 1).await;
        });
    }

    #[test]
    #[should_panic(expected = "Other threads might still be using it")]
    fn test_wait_arrive_different_target_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive();
            wg.arrive();
            wg.arrive();
            wg.wait_arrive(3).await.unwrap();
            let _ = wg.wait_arrive(5).await;
        });
    }

    #[test]
    #[should_panic(expected = "Other threads might still be using it")]
    fn test_arrive_after_wait_arrive_completed_panics() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            wg.arrive();
            wg.wait_arrive(1).await.unwrap();
            wg.arrive();
        });
    }

    #[test]
    fn test_arrive_during_wait_arrive_completes() {
        let rt = current_thread_runtime();
        rt.block_on(async {
            let wg = WaitGroupArrive::new();
            let wg2 = wg.clone();

            let waiter = tokio::spawn(async move {
                wg2.wait_arrive(2).await.unwrap();
            });

            tokio::task::yield_now().await;
            wg.arrive();
            wg.arrive();
            assert!(waiter.await.is_ok());
        });
    }
}
