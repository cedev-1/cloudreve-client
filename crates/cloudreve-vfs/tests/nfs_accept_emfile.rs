//! Its own test binary: it lowers the process's file descriptor limit.
//!
//! The loopback NFS server used to return from its accept loop on the
//! first accept error — EMFILE after a wake from sleep was enough (a GUI
//! app starts with 256 descriptors) — and nobody noticed: the mount just
//! stopped answering. Running out of descriptors for a moment must not kill
//! it.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudreve_vfs::nfs::{start_server, VfsNfs};
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::VfsTestEnv;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn open_fd_count() -> u64 {
    std::fs::read_dir("/dev/fd").map(|d| d.count() as u64).unwrap_or(64)
}

fn set_soft_nofile(soft: u64) -> u64 {
    // SAFETY: plain syscalls on a stack struct.
    unsafe {
        let mut lim: libc::rlimit = std::mem::zeroed();
        libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim);
        let old = lim.rlim_cur as u64;
        lim.rlim_cur = soft as libc::rlim_t;
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &lim), 0);
        old
    }
}

/// NFSv3 NULL ping, AUTH_NULL; true if the server answered.
async fn ping(port: u16) -> bool {
    let Ok(Ok(mut sock)) =
        tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(("127.0.0.1", port))).await
    else {
        return false;
    };
    let mut msg = Vec::new();
    for word in [7u32, 0, 2, 100_003, 3, 0, 0, 0, 0, 0] {
        msg.extend_from_slice(&word.to_be_bytes());
    }
    let mut framed = (0x8000_0000u32 | msg.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&msg);
    if sock.write_all(&framed).await.is_err() {
        return false;
    }
    let mut header = [0u8; 4];
    matches!(tokio::time::timeout(Duration::from_secs(5), sock.read_exact(&mut header)).await, Ok(Ok(_)))
}

#[tokio::test]
async fn the_server_survives_running_out_of_file_descriptors() {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let server = start_server(VfsNfs::new(Arc::new(vfs)), "test".into()).await.unwrap();
    let stopped = server.stopped();
    assert!(ping(server.port).await, "precondition: the server answers");

    let old = set_soft_nofile(open_fd_count() + 8);
    let mut hoard = Vec::new();
    while let Ok(sock) = std::net::TcpStream::connect(("127.0.0.1", server.port)) {
        hoard.push(sock);
        if hoard.len() > 64 {
            break;
        }
    }
    // Let the accept loop run into EMFILE on the backlog.
    tokio::time::sleep(Duration::from_millis(600)).await;
    set_soft_nofile(old);
    drop(hoard);
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(stopped.borrow().is_none(), "the server stopped: {:?}", *stopped.borrow());
    assert!(ping(server.port).await, "the server no longer answers after running out of descriptors");

    // A GUI app starts with a soft limit of 256: mounting raises it so the
    // server has room (`mount.rs`, `raise_open_file_limit`). Here, not in a
    // test of its own: tests of one binary run in parallel.
    #[cfg(target_os = "macos")]
    {
        let old = set_soft_nofile(256);
        cloudreve_vfs::mount::raise_open_file_limit_for_tests();
        let now = set_soft_nofile(old);
        assert!(now >= 4096, "soft limit still {now}");
    }
}

