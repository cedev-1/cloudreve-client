//! Who may talk to the loopback NFS server behind a mounted drive.
//!
//! The server listens on `127.0.0.1`, so the network cannot reach it — but
//! every local process can: another account on the same Mac, or a
//! sandboxed app of this user allowed outgoing connections. Before this,
//! any of them could MNT `/`, get the root handle, and read, write or
//! delete every cloud file. These tests speak raw ONC RPC over TCP to the
//! server exactly as `mount.rs` starts it (`nfs::start_server`).

mod common;

use std::sync::Arc;

use cloudreve_vfs::nfs::{start_server, VfsNfs};
use cloudreve_vfs::vfs::{Vfs, DEFAULT_CACHE_MAX_BYTES};
use common::VfsTestEnv;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const NFS_PROGRAM: u32 = 100_003;
const MOUNT_PROGRAM: u32 = 100_005;
const AUTH_NULL: u32 = 0;
const AUTH_UNIX: u32 = 1;
const NFS3ERR_ACCES: u32 = 13;
const MNT3ERR_ACCES: u32 = 13;

#[derive(Clone, Copy)]
enum Cred {
    Null,
    Unix(u32),
}

fn me() -> u32 {
    // SAFETY: getuid never fails and takes no arguments.
    unsafe { libc::getuid() }
}

/// A uid that is neither this user's nor root's: another account.
fn someone_else() -> u32 {
    if me() == 501 { 502 } else { 501 }
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_opaque(buf: &mut Vec<u8>, data: &[u8]) {
    put_u32(buf, data.len() as u32);
    buf.extend_from_slice(data);
    buf.resize(buf.len() + (4 - data.len() % 4) % 4, 0);
}

/// Each call gets its own xid: the server drops a repeated (client, xid) as
/// a retransmission.
fn next_xid() -> u32 {
    static XID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    XID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn call(_xid: u32, prog: u32, vers: u32, proc_: u32, cred: Cred, args: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    put_u32(&mut b, next_xid());
    put_u32(&mut b, 0); // CALL
    put_u32(&mut b, 2); // RPC version
    put_u32(&mut b, prog);
    put_u32(&mut b, vers);
    put_u32(&mut b, proc_);
    match cred {
        Cred::Null => {
            put_u32(&mut b, AUTH_NULL);
            put_u32(&mut b, 0);
        }
        Cred::Unix(uid) => {
            let mut body = Vec::new();
            put_u32(&mut body, 0); // stamp
            put_opaque(&mut body, b"localhost");
            put_u32(&mut body, uid);
            put_u32(&mut body, 20); // gid
            put_u32(&mut body, 0); // no extra gids
            put_u32(&mut b, AUTH_UNIX);
            put_opaque(&mut b, &body);
        }
    }
    put_u32(&mut b, AUTH_NULL); // verifier
    put_u32(&mut b, 0);
    b.extend_from_slice(args);
    b
}

struct Reply {
    accepted: bool,
    accept_stat: u32,
    body: Vec<u8>,
}

impl Reply {
    fn u32_at(&self, word: usize) -> u32 {
        u32::from_be_bytes(self.body[word * 4..word * 4 + 4].try_into().unwrap())
    }
}

async fn rpc(sock: &mut TcpStream, msg: Vec<u8>) -> Reply {
    sock.write_all(&(0x8000_0000u32 | msg.len() as u32).to_be_bytes()).await.unwrap();
    sock.write_all(&msg).await.unwrap();
    let mut header = [0u8; 4];
    tokio::time::timeout(std::time::Duration::from_secs(10), sock.read_exact(&mut header))
        .await
        .expect("no reply within 10 s")
        .unwrap();
    let len = (u32::from_be_bytes(header) & 0x7fff_ffff) as usize;
    let mut data = vec![0u8; len];
    sock.read_exact(&mut data).await.unwrap();
    let word = |i: usize| u32::from_be_bytes(data[i * 4..i * 4 + 4].try_into().unwrap());
    assert_eq!(word(1), 1, "not a REPLY");
    if word(2) != 0 {
        return Reply { accepted: false, accept_stat: u32::MAX, body: data[12..].to_vec() };
    }
    // verifier: flavor, length, body
    let verf_len = word(4) as usize;
    let at = 20 + verf_len.div_ceil(4) * 4;
    let accept_stat = u32::from_be_bytes(data[at..at + 4].try_into().unwrap());
    Reply { accepted: true, accept_stat, body: data[at + 4..].to_vec() }
}

fn mnt_args() -> Vec<u8> {
    let mut a = Vec::new();
    put_opaque(&mut a, b"/");
    a
}

/// MNT `/`; `Ok((root handle, auth flavors))` or `Err(mountstat3)`.
async fn mnt(sock: &mut TcpStream, cred: Cred) -> Result<(Vec<u8>, Vec<u32>), u32> {
    let r = rpc(sock, call(1, MOUNT_PROGRAM, 3, 1, cred, &mnt_args())).await;
    assert!(r.accepted && r.accept_stat == 0, "MNT must be answered, not rejected at the RPC level");
    let status = r.u32_at(0);
    if status != 0 {
        return Err(status);
    }
    let fh_len = r.u32_at(1) as usize;
    let fh = r.body[8..8 + fh_len].to_vec();
    let at = 8 + fh_len.div_ceil(4) * 4;
    let n = u32::from_be_bytes(r.body[at..at + 4].try_into().unwrap()) as usize;
    let flavors = (0..n)
        .map(|i| u32::from_be_bytes(r.body[at + 4 + i * 4..at + 8 + i * 4].try_into().unwrap()))
        .collect();
    Ok((fh, flavors))
}

/// NFSv3 GETATTR; returns the nfsstat3.
async fn getattr(sock: &mut TcpStream, cred: Cred, fh: &[u8]) -> u32 {
    let mut a = Vec::new();
    put_opaque(&mut a, fh);
    let r = rpc(sock, call(2, NFS_PROGRAM, 3, 1, cred, &a)).await;
    assert!(r.accepted && r.accept_stat == 0, "GETATTR must be answered with an nfsstat3");
    r.u32_at(0)
}

/// NFSv3 ACCESS asking for every bit; `(nfsstat3, granted bits)`.
async fn access(sock: &mut TcpStream, cred: Cred, fh: &[u8]) -> (u32, Option<u32>) {
    let mut a = Vec::new();
    put_opaque(&mut a, fh);
    put_u32(&mut a, 0x3f);
    let r = rpc(sock, call(3, NFS_PROGRAM, 3, 4, cred, &a)).await;
    assert!(r.accepted && r.accept_stat == 0);
    let status = r.u32_at(0);
    if status != 0 {
        return (status, None);
    }
    // post_op_attr: present flag + 84-byte fattr3
    let at = if r.u32_at(1) == 1 { 4 + 4 + 84 } else { 8 };
    (status, Some(u32::from_be_bytes(r.body[at..at + 4].try_into().unwrap())))
}

async fn server() -> (VfsTestEnv, Arc<Vfs>, u16, cloudreve_vfs::nfs::NfsServer) {
    let env = VfsTestEnv::new().await;
    env.set_remote_files(vec![common::remote_file("secret.txt", 6, "e1")]).await;
    let (vfs, _rx) =
        Vfs::new(env.client(), common::REMOTE_BASE.into(), env.cache_dir(), DEFAULT_CACHE_MAX_BYTES)
            .unwrap();
    let vfs = Arc::new(vfs);
    let server = start_server(VfsNfs::new(vfs.clone()), "test".into()).await.unwrap();
    (env, vfs, server.port, server)
}

async fn connect(port: u16) -> TcpStream {
    TcpStream::connect(("127.0.0.1", port)).await.unwrap()
}

#[tokio::test]
async fn mnt_without_credentials_is_refused() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    assert_eq!(mnt(&mut sock, Cred::Null).await.err(), Some(MNT3ERR_ACCES));
}

#[tokio::test]
async fn mnt_from_another_account_is_refused() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    assert_eq!(mnt(&mut sock, Cred::Unix(someone_else())).await.err(), Some(MNT3ERR_ACCES));
}

/// What `mount_nfs` does: one MNT as the user. It must work, and offer
/// AUTH_UNIX only — offered AUTH_NULL first, a client could pick it and
/// then be refused on every call.
#[tokio::test]
async fn the_first_mnt_as_this_user_gets_the_root_handle_and_auth_unix_only() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    let (fh, flavors) = mnt(&mut sock, Cred::Unix(me())).await.expect("the user's own mount must work");
    assert!(!fh.is_empty());
    assert_eq!(flavors, vec![AUTH_UNIX]);
}

/// AUTH_UNIX is a claim, not a proof: a local process can put this user's
/// uid in its call. Only the mount itself ever needs MNT, once — any later
/// MNT (the attacker's, uid forged or not) is refused, so the root handle
/// never leaves the kernel.
#[tokio::test]
async fn a_second_mnt_is_refused_even_with_the_users_uid() {
    let (_env, _vfs, port, _server) = server().await;
    let mut first = connect(port).await;
    mnt(&mut first, Cred::Unix(me())).await.expect("first mount");
    let mut attacker = connect(port).await;
    assert_eq!(mnt(&mut attacker, Cred::Unix(me())).await.err(), Some(MNT3ERR_ACCES));
}

/// Without MNT, the only way in is guessing a handle. Upstream seeds the
/// handle generation with the bind time in milliseconds — guessable within
/// a few thousand tries. It must be random.
#[tokio::test]
async fn the_handle_generation_is_not_the_bind_time() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    let (fh, _) = mnt(&mut sock, Cred::Unix(me())).await.unwrap();
    let generation = u64::from_le_bytes(fh[..8].try_into().unwrap());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let day_ms = 86_400_000;
    assert!(
        generation.abs_diff(now_ms) > day_ms,
        "generation {generation} is a timestamp ({now_ms} now)"
    );
}

/// Through the kernel mount, each call carries the real uid of the process
/// making it: another account reaching the mount is refused on every call.
#[tokio::test]
async fn calls_from_another_account_or_without_credentials_are_refused() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    let (fh, _) = mnt(&mut sock, Cred::Unix(me())).await.unwrap();

    assert_eq!(getattr(&mut sock, Cred::Null, &fh).await, NFS3ERR_ACCES);
    assert_eq!(getattr(&mut sock, Cred::Unix(someone_else()), &fh).await, NFS3ERR_ACCES);
    assert_eq!(access(&mut sock, Cred::Unix(someone_else()), &fh).await.0, NFS3ERR_ACCES);
}

/// The user's own apps, and root (Spotlight's `mds`, backups), work as
/// before; ACCESS grants them everything they ask.
#[tokio::test]
async fn calls_from_this_user_and_root_are_served() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    let (fh, _) = mnt(&mut sock, Cred::Unix(me())).await.unwrap();

    assert_eq!(getattr(&mut sock, Cred::Unix(me()), &fh).await, 0);
    assert_eq!(getattr(&mut sock, Cred::Unix(0), &fh).await, 0);
    assert_eq!(access(&mut sock, Cred::Unix(me()), &fh).await, (0, Some(0x3f)));
}

/// The NULL procedure (a ping, no data) stays open: clients probe with it,
/// sometimes without credentials.
#[tokio::test]
async fn the_null_ping_is_answered_without_credentials() {
    let (_env, _vfs, port, _server) = server().await;
    let mut sock = connect(port).await;
    let r = rpc(&mut sock, call(9, NFS_PROGRAM, 3, 0, Cred::Null, &[])).await;
    assert!(r.accepted && r.accept_stat == 0);
}

/// Unmounting aborts the server. Upstream left every accepted connection
/// running in a detached task: a client that got in kept being served by
/// a server that, for the app, no longer existed.
#[tokio::test]
async fn connections_close_when_the_server_is_stopped() {
    let (_env, _vfs, port, server) = server().await;
    let mut sock = connect(port).await;
    let (fh, _) = mnt(&mut sock, Cred::Unix(me())).await.unwrap();
    assert_eq!(getattr(&mut sock, Cred::Unix(me()), &fh).await, 0);

    server.abort();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut a = Vec::new();
    put_opaque(&mut a, &fh);
    let msg = call(0, NFS_PROGRAM, 3, 1, Cred::Unix(me()), &a);
    let _ = sock.write_all(&(0x8000_0000u32 | msg.len() as u32).to_be_bytes()).await;
    let _ = sock.write_all(&msg).await;
    let mut header = [0u8; 4];
    let answered = matches!(
        tokio::time::timeout(std::time::Duration::from_secs(3), sock.read_exact(&mut header)).await,
        Ok(Ok(_))
    );
    assert!(!answered, "a connection outlived its server");
}
