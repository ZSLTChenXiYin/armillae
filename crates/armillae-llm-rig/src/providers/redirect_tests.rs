use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use armillae_core::{CompletionRequest, Message};
use armillae_llm::{
    BoxFuture, BridgeConfig, BridgeError, BridgeFactory, BridgeResolveContext, CredentialRef,
    SecretResolver, SecretString, TransportConfig, TransportErrorKind,
};
use futures::StreamExt;

use crate::RigBridgeFactory;

struct TestSecret;
impl SecretResolver for TestSecret {
    fn resolve<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<SecretString, BridgeError>> {
        Box::pin(async { Ok(SecretString::from("redirect-test-only")) })
    }
}

// Final 418 deliberately avoids provider response parsing: reaching it proves
// the real client followed exactly the intended number of HTTP redirects.
#[tokio::test]
async fn all_provider_call_paths_enforce_redirect_limits() {
    for provider in [
        "openai",
        "openai-compatible",
        "anthropic",
        "deepseek",
        "minimax",
        "moonshot",
        "ollama",
    ] {
        for streaming in [false, true] {
            for (limit, redirects, expected_status, expected_hits) in
                [(0, 1, Some(307), 1), (2, 2, Some(418), 3), (1, 2, None, 2)]
            {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let address = listener.local_addr().unwrap();
                let stop = Arc::new(AtomicBool::new(false));
                let hits = Arc::new(AtomicUsize::new(0));
                let server_stop = stop.clone();
                let server_hits = hits.clone();
                let server = std::thread::spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while !server_stop.load(Ordering::SeqCst) && Instant::now() < deadline {
                        let (mut socket, _) = match listener.accept() {
                            Ok(connection) => connection,
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(1));
                                continue;
                            }
                            Err(error) => panic!("accept: {error}"),
                        };
                        socket.set_nonblocking(false).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let mut data = Vec::new();
                        loop {
                            let mut buffer = [0; 4096];
                            let n = socket.read(&mut buffer).unwrap();
                            assert_ne!(n, 0);
                            data.extend_from_slice(&buffer[..n]);
                            if let Some(end) = data.windows(4).position(|part| part == b"\r\n\r\n")
                            {
                                let headers = String::from_utf8_lossy(&data[..end]);
                                let length = headers
                                    .lines()
                                    .find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse::<usize>().unwrap())
                                    })
                                    .unwrap_or(0);
                                if data.len() >= end + 4 + length {
                                    break;
                                }
                            }
                        }
                        let hit = server_hits.fetch_add(1, Ordering::SeqCst);
                        if hit < redirects {
                            write!(socket, "HTTP/1.1 307 Temporary Redirect\r\nLocation: /next/{hit}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                        } else {
                            write!(socket, "HTTP/1.1 418 Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                        }
                    }
                });
                let config = BridgeConfig::builder(provider, "test-model")
                    .endpoint(format!("http://{address}").parse().unwrap())
                    .credential(CredentialRef::Resolver { key: "test".into() })
                    .transport(TransportConfig {
                        max_redirects: limit,
                        request_timeout_ms: 5_000,
                        ..Default::default()
                    })
                    .build()
                    .unwrap();
                let resolved = config
                    .resolve_with(BridgeResolveContext::new().secret_resolver(&TestSecret))
                    .await
                    .unwrap();
                let bridge = RigBridgeFactory.create(resolved).await.unwrap();
                let request = CompletionRequest {
                    messages: vec![Message::user("test")],
                    generation: armillae_core::GenerationOptions {
                        max_output_tokens: Some(16),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                let error = if streaming {
                    match bridge.stream(request).await {
                        Err(error) => error,
                        Ok(mut stream) => {
                            let mut failure = None;
                            while let Some(event) = stream.next().await {
                                if let Err(error) = event {
                                    failure = Some(error);
                                    break;
                                }
                            }
                            assert!(
                                stream.next().await.is_none(),
                                "failure must terminate the stream"
                            );
                            failure.expect("HTTP failures must reach the stream caller")
                        }
                    }
                } else {
                    bridge.complete(request).await.expect_err("HTTP failure")
                };
                stop.store(true, Ordering::SeqCst);
                server.join().unwrap();
                assert_eq!(
                    hits.load(Ordering::SeqCst),
                    expected_hits,
                    "{provider} streaming={streaming} limit={limit}"
                );
                let metadata = match error {
                    BridgeError::ProviderRejected { metadata, .. }
                    | BridgeError::StreamInterrupted { metadata }
                    | BridgeError::Transport { metadata, .. } => metadata,
                    other => panic!("unexpected {provider} error: {other:?}"),
                };
                assert_eq!(metadata.http_status, expected_status, "{provider}");
                if expected_status.is_none() {
                    assert_eq!(
                        metadata.transport_kind,
                        if streaming && provider != "ollama" {
                            None
                        } else {
                            Some(TransportErrorKind::Redirect)
                        },
                        "{provider} streaming={streaming}"
                    );
                }
            }
        }
    }
}
