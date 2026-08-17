//! Session establishment: the error path, and the readiness contract.
//!
//! Both sides used to defer their socket setup into the spawned session task and
//! `.unwrap()` it there, which lost two things.
//!
//! The cause. A refused TCP connect panicked a tokio worker from inside
//! `SmppClient::start`'s task. The panic did not close the bind channel (the
//! listener holds the sender), so the caller waited out its whole bind timeout
//! and then reported "bind timed out" — a symptom that named neither the host
//! nor the refusal. `on_connection_failed` now carries the real reason.
//!
//! The readiness. `SmppServer::start` returned before `TcpListener::bind` had
//! run, so "the server is started" did not mean "the server is accepting".
//! Every test in this crate worked around it with a `sleep(100ms)`; the Python
//! suite did not sleep and duly raced, failing under free-threaded CPython in
//! CI. `start` now binds before it returns, so the contract holds without a
//! sleep and there is no window left to race.

use std::net::{IpAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use smpp34::client::{SmppClient, SmppClientListener, BIND_TYPE};
use smpp34::{SmppServer, SmppServerListener};
use tokio::sync::mpsc;

/// A port that nothing is listening on: bind it to learn a free number, then
/// drop the listener so a connect to it is refused rather than accepted.
fn closed_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = l.local_addr().expect("local_addr").port();
    drop(l);
    port
}

/// Records whatever the session-establishment failure hook is told.
struct FailureRecorder {
    failures: mpsc::Sender<String>,
}

#[async_trait]
impl SmppClientListener for FailureRecorder {
    async fn on_connection_failed(&self, error: &str) {
        let _ = self.failures.send(error.to_string()).await;
    }
}

#[async_trait]
impl SmppServerListener for FailureRecorder {
    async fn on_listen_failed(&self, error: &str) {
        let _ = self.failures.send(error.to_string()).await;
    }
}

fn client_to(port: u16, listener: Arc<FailureRecorder>) -> SmppClient {
    SmppClient::new(
        "127.0.0.1".to_string(),
        port,
        false,
        BIND_TYPE::TRX,
        "u".to_string(),
        "p".to_string(),
        String::new(),
        1,
        1,
        String::new(),
        listener,
        16,
    )
}

// ── The client's connect error path ─────────────────────────────────────────

/// A refused connect must reach the listener as a refused connect, naming the
/// address, and must not panic the session task.
#[tokio::test]
async fn a_refused_connect_reports_its_real_cause() {
    let port = closed_port();
    let (tx, mut rx) = mpsc::channel(4);
    let mut client = client_to(port, Arc::new(FailureRecorder { failures: tx }));

    client.start().await;

    let reason = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("on_connection_failed was never called")
        .expect("failure channel closed");

    let lowered = reason.to_lowercase();
    assert!(
        lowered.contains("connect"),
        "the reason should say the connect failed, got: {reason}"
    );
    assert!(
        lowered.contains("refused"),
        "the reason should carry the OS refusal, got: {reason}"
    );
    assert!(
        reason.contains(&port.to_string()),
        "the reason should name the port that refused, got: {reason}"
    );
}

/// A client that never connected is not alive, and reports promptly rather than
/// leaving the caller to wait out a bind timeout.
#[tokio::test]
async fn a_refused_connect_fails_fast_and_leaves_the_client_not_alive() {
    let port = closed_port();
    let (tx, _rx) = mpsc::channel(4);
    let mut client = client_to(port, Arc::new(FailureRecorder { failures: tx }));

    let started = Instant::now();
    client.start().await;
    let elapsed = started.elapsed();

    assert!(
        !client.is_alive(),
        "a client that never connected must not report itself alive"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "a refused connect should surface immediately, took {elapsed:?}"
    );
}

// ── The server's readiness contract ────────────────────────────────────────

/// When `start()` returns, the listener is accepting. No sleep, no retry: a
/// connect issued on the very next line must succeed.
#[tokio::test]
async fn server_start_is_accepting_by_the_time_it_returns() {
    let port = closed_port();
    let (tx, _rx) = mpsc::channel(4);
    let mut server = SmppServer::new(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(FailureRecorder { failures: tx }),
    );

    server.start().await;

    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("start() returned but the listener was not accepting yet");
    assert!(server.is_alive(), "a listening server should report alive");
}

/// A port already taken must reach the listener instead of panicking the task.
#[tokio::test]
async fn a_bind_conflict_reports_its_real_cause() {
    let held = StdTcpListener::bind("127.0.0.1:0").expect("hold a port");
    let port = held.local_addr().expect("local_addr").port();

    let (tx, mut rx) = mpsc::channel(4);
    let mut server = SmppServer::new(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(FailureRecorder { failures: tx }),
    );

    server.start().await;

    let reason = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("on_listen_failed was never called")
        .expect("failure channel closed");

    assert!(
        reason.to_lowercase().contains("bind"),
        "the reason should say the bind failed, got: {reason}"
    );
    assert!(
        reason.contains(&port.to_string()),
        "the reason should name the port, got: {reason}"
    );
    assert!(
        !server.is_alive(),
        "a server that never bound must not report itself alive"
    );
}

// ── stop() after a failed start ─────────────────────────────────────────────
//
// `start()` returning without spawning is a state that only exists because a
// failed connect/bind no longer panics, so `stop()` had never had to cope with
// it — and `self.handle.take().expect("Called stop on non-running thread")`
// would have panicked on exactly the instance a caller is most likely to clean
// up. `Drop` calls `stop()` too, so this also covers letting one go out of scope.

#[tokio::test]
async fn stopping_a_client_that_never_connected_is_a_no_op() {
    let port = closed_port();
    let (tx, _rx) = mpsc::channel(4);
    let mut client = client_to(port, Arc::new(FailureRecorder { failures: tx }));

    client.start().await;
    assert!(!client.is_alive());

    // Must not panic, and must stay callable.
    client.stop().await;
    client.stop().await;
}

#[tokio::test]
async fn stopping_a_server_that_never_bound_is_a_no_op() {
    let held = StdTcpListener::bind("127.0.0.1:0").expect("hold a port");
    let port = held.local_addr().expect("local_addr").port();

    let (tx, _rx) = mpsc::channel(4);
    let mut server = SmppServer::new(
        IpAddr::from([127, 0, 0, 1]),
        port,
        Arc::new(FailureRecorder { failures: tx }),
    );

    server.start().await;
    assert!(!server.is_alive());

    server.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn dropping_a_client_that_never_connected_does_not_panic() {
    let port = closed_port();
    let (tx, _rx) = mpsc::channel(4);
    {
        let mut client = client_to(port, Arc::new(FailureRecorder { failures: tx }));
        client.start().await;
    } // Drop runs here and calls stop() when alive; must not panic either way.
}
