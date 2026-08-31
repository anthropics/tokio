#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", not(target_os = "wasi"), not(miri)))]

//! `Builder::io_shards`: I/O and timers stay live when the multi-thread
//! runtime polls several epoll instances.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn rt(workers: usize, shards: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .io_shards(shards)
        .enable_all()
        .build()
        .unwrap()
}

async fn echo_round_trips(conns: usize, rounds: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let mut tasks = Vec::new();
    for c in 0..conns {
        tasks.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let mut buf = [0u8; 8];
            for r in 0..rounds {
                let msg = ((c * rounds + r) as u64).to_le_bytes();
                s.write_all(&msg).await.unwrap();
                s.read_exact(&mut buf).await.unwrap();
                assert_eq!(buf, msg);
                if r % 16 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .expect("stalled")
            .unwrap();
    }
}

#[test]
fn echo_across_shards() {
    for shards in [1, 2, 4] {
        rt(8, shards).block_on(echo_round_trips(32, 64));
    }
}

#[test]
fn more_shards_than_needed_is_clamped() {
    // 2 workers cannot host 4 shards; the builder clamps instead of failing.
    rt(2, 4).block_on(echo_round_trips(4, 16));
}

#[test]
fn timers_fire_with_all_workers_parked() {
    let rt = rt(8, 4);
    rt.block_on(async {
        let mut tasks = Vec::new();
        for i in 0..256u64 {
            tasks.push(tokio::spawn(tokio::time::sleep(Duration::from_millis(
                i % 50,
            ))));
        }
        for t in tasks {
            tokio::time::timeout(Duration::from_secs(10), t)
                .await
                .expect("timer lost")
                .unwrap();
        }
    });
}

#[test]
fn busy_runtime_still_polls_its_shards() {
    // Every worker permanently busy: I/O must still be driven from the
    // maintenance tick (regression: the help sweep once suppressed the
    // zero-timeout poll of the worker's own shard).
    for shards in [1, 2] {
        let rt = rt(2, shards);
        for _ in 0..64 {
            rt.spawn(async {
                loop {
                    std::future::poll_fn::<(), _>(|cx| {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    })
                    .await;
                }
            });
        }
        rt.block_on(echo_round_trips(4, 16));
    }
}
