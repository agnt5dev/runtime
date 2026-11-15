//! One bounded signed HTTP attempt. Retry state belongs to the journal work
//! item; no detached retry loop or process-local delivery queue lives here.
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

pub const MAX_BODY_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug)]
pub enum DestinationPolicy {
    PublicHttps,
    /// Runtime-configured control-plane endpoint, never supplied by a caller.
    /// V1 Evals callbacks may use the private service network.
    ManagedCallback {
        origin: String,
        path_prefix: String,
    },
}

pub enum Signature<'a> {
    Webhook {
        delivery_id: &'a str,
        key_id: &'a str,
        secret: &'a [u8],
    },
    EvalCallback {
        secret: &'a [u8],
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryResult {
    Delivered,
    Retry {
        after: Duration,
        status: Option<u16>,
    },
    Rejected {
        status: Option<u16>,
        reason: &'static str,
    },
}

pub fn validate_url(raw: &str, policy: &DestinationPolicy) -> Result<reqwest::Url, &'static str> {
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid destination URL")?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err("destination credentials and fragments are forbidden");
    }
    match policy {
        DestinationPolicy::PublicHttps if url.scheme() != "https" || url.query().is_some() => {
            return Err("webhooks require HTTPS without query credentials")
        }
        DestinationPolicy::PublicHttps => {}
        DestinationPolicy::ManagedCallback {
            origin,
            path_prefix,
        } => {
            let trusted = reqwest::Url::parse(origin)
                .map_err(|_| "invalid managed callback configuration")?;
            if !matches!(url.scheme(), "http" | "https")
                || url.origin() != trusted.origin()
                || !url.path().starts_with(path_prefix)
                || url.query().is_some()
            {
                return Err("callback does not match the configured control-plane destination");
            }
        }
    }
    Ok(url)
}

/// Public delivery blocks every non-global range, including mapped addresses.
/// DNS answers are checked in full and pinned to the actual connection below.
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => public_v6(ip),
    }
}
fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && (b == 168 || b == 0 || (b == 88 && c == 99)))
        || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
        || (a == 203 && b == 0 && c == 113))
}
fn public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return public_v4(v4);
    }
    let segments = ip.segments();
    // Admit global unicast only. Exclude protocol assignment, documentation,
    // 6to4 (which can tunnel private IPv4), and ORCHID/benchmarking space.
    (segments[0] & 0xe000) == 0x2000
        && !(segments[0] == 0x2001 && segments[1] < 0x0200)
        && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        && segments[0] != 0x2002
        && !(segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
}

fn mac(secret: &[u8], parts: &[&[u8]]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    let bytes = mac.finalize().into_bytes();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn webhook_signature(
    secret: &[u8],
    key_id: &str,
    delivery_id: &str,
    timestamp: i64,
    body: &[u8],
) -> String {
    mac(
        secret,
        &[
            b"v1.",
            key_id.as_bytes(),
            b".",
            delivery_id.as_bytes(),
            b".",
            timestamp.to_string().as_bytes(),
            b".",
            body,
        ],
    )
}

fn valid_signature_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub async fn send_once(
    url: &str,
    policy: &DestinationPolicy,
    body: Vec<u8>,
    signature: Signature<'_>,
) -> DeliveryResult {
    use DeliveryResult::*;
    let url = match validate_url(url, policy) {
        Ok(url) => url,
        Err(reason) => {
            return Rejected {
                status: None,
                reason,
            }
        }
    };
    if body.len() > MAX_BODY_BYTES {
        return Rejected {
            status: None,
            reason: "delivery body exceeds limit",
        };
    }
    let host = url
        .host_str()
        .expect("validated host")
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = url.port_or_known_default().expect("validated HTTP scheme");
    let addresses = match tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host((host, port)),
    )
    .await
    {
        Ok(Ok(addresses)) => addresses.collect::<Vec<SocketAddr>>(),
        _ => {
            return Retry {
                after: Duration::from_secs(5),
                status: None,
            }
        }
    };
    if addresses.is_empty() {
        return Retry {
            after: Duration::from_secs(5),
            status: None,
        };
    }
    if addresses.len() > 32
        || (matches!(policy, DestinationPolicy::PublicHttps)
            && addresses
                .iter()
                .any(|address| !public_address(address.ip())))
    {
        return Rejected {
            status: None,
            reason: "destination address is not allowed",
        };
    }
    let client = match reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .resolve_to_addrs(host, &addresses)
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return Retry {
                after: Duration::from_secs(5),
                status: None,
            }
        }
    };
    let timestamp = (crate::work::now_ms() / 1000).to_string();
    let mut request = client.post(url).header("content-type", "application/json");
    request = match signature {
        Signature::Webhook {
            delivery_id,
            key_id,
            secret,
        } => {
            if secret.len() < 32 || !valid_signature_id(delivery_id) || !valid_signature_id(key_id)
            {
                return Rejected {
                    status: None,
                    reason: "invalid webhook signing material",
                };
            }
            let signed = mac(
                secret,
                &[
                    b"v1.",
                    key_id.as_bytes(),
                    b".",
                    delivery_id.as_bytes(),
                    b".",
                    timestamp.as_bytes(),
                    b".",
                    &body,
                ],
            );
            request
                .header("agnt5-webhook-id", delivery_id)
                .header("agnt5-webhook-timestamp", &timestamp)
                .header("agnt5-webhook-key-id", key_id)
                .header("agnt5-webhook-signature", format!("v1={signed}"))
        }
        Signature::EvalCallback { secret } => {
            if secret.is_empty() {
                return Rejected {
                    status: None,
                    reason: "missing callback signing material",
                };
            }
            request
                .header(
                    "x-callback-signature",
                    mac(secret, &[timestamp.as_bytes(), b".", &body]),
                )
                .header("x-callback-timestamp", &timestamp)
        }
    };
    let response = match request.body(body).send().await {
        Ok(response) => response,
        Err(_) => {
            return Retry {
                after: Duration::from_secs(5),
                status: None,
            }
        }
    };
    let status = response.status();
    // Do not read or log untrusted response bodies. Dropping the bounded
    // per-attempt connection prevents oversized responses from consuming RAM.
    if status.is_success() {
        return Delivered;
    }
    if status.is_server_error() || matches!(status.as_u16(), 408 | 425 | 429) {
        let seconds = response
            .headers()
            .get("retry-after")
            .and_then(|header| header.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5)
            .clamp(1, 3600);
        return Retry {
            after: Duration::from_secs(seconds),
            status: Some(status.as_u16()),
        };
    }
    Rejected {
        status: Some(status.as_u16()),
        reason: "destination rejected delivery",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_private_and_special_destination_addresses() {
        for ip in [
            "0.1.2.3",
            "127.0.0.1",
            "10.0.0.1",
            "100.100.100.200",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.0.1",
            "192.0.2.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
            "2002:7f00:1::",
            "64:ff9b::7f00:1",
            "3fff::1",
        ] {
            assert!(!public_address(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
            assert!(public_address(ip.parse().unwrap()));
        }
    }
    #[test]
    fn callback_exception_does_not_authorize_other_origins_or_paths() {
        let policy = DestinationPolicy::ManagedCallback {
            origin: "http://control-plane:8080".into(),
            path_prefix: "/api/v1/internal/experiment-runs/abc/".into(),
        };
        assert!(validate_url(
            "http://control-plane:8080/api/v1/internal/experiment-runs/abc/results",
            &policy
        )
        .is_ok());
        for url in [
            "http://other:8080/api/v1/internal/experiment-runs/abc/results",
            "http://control-plane:8080/admin",
            "http://control-plane:8080/api/v1/internal/experiment-runs/abc/../other/results",
            "http://user:secret@control-plane:8080/api/v1/internal/experiment-runs/abc/results",
        ] {
            assert!(validate_url(url, &policy).is_err());
        }
        assert!(validate_url("http://example.com", &DestinationPolicy::PublicHttps).is_err());
    }
    #[test]
    fn signature_matches_language_neutral_utf8_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/contracts/fixtures/outbound-webhook-signature-v1.json"
        ))
        .unwrap();
        let key_hex = fixture["key_hex"].as_str().unwrap();
        let key: Vec<u8> = key_hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        assert_eq!(
            webhook_signature(
                &key,
                fixture["key_id"].as_str().unwrap(),
                fixture["delivery_id"].as_str().unwrap(),
                fixture["timestamp"].as_i64().unwrap(),
                fixture["body"].as_str().unwrap().as_bytes(),
            ),
            fixture["signature"].as_str().unwrap()
        );
    }

    #[tokio::test]
    async fn public_delivery_rejects_metadata_before_connecting() {
        assert_eq!(
            send_once(
                "https://169.254.169.254/latest/meta-data",
                &DestinationPolicy::PublicHttps,
                b"{}".to_vec(),
                Signature::Webhook {
                    delivery_id: "delivery",
                    key_id: "key",
                    secret: &[1; 32]
                },
            )
            .await,
            DeliveryResult::Rejected {
                status: None,
                reason: "destination address is not allowed"
            }
        );
    }

    #[tokio::test]
    async fn real_receiver_sees_signed_bytes_and_redirects_are_not_followed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let policy = DestinationPolicy::ManagedCallback {
            origin: origin.clone(),
            path_prefix: "/events/".into(),
        };
        let body = "{\"message\":\"héllo\"}".as_bytes().to_vec();
        let expected_body = body.clone();
        let server = tokio::spawn(async move {
            for status in ["429 Too Many Requests", "302 Found", "204 No Content"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut wire = Vec::new();
                loop {
                    let mut buf = [0; 2048];
                    let count = socket.read(&mut buf).await.unwrap();
                    assert_ne!(count, 0);
                    wire.extend_from_slice(&buf[..count]);
                    if wire
                        .windows(4)
                        .position(|part| part == b"\r\n\r\n")
                        .is_some_and(|start| wire.len() >= start + 4 + expected_body.len())
                    {
                        break;
                    }
                }
                let start = wire
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                    .unwrap();
                assert_eq!(&wire[start + 4..], expected_body);
                let headers = std::str::from_utf8(&wire[..start]).unwrap();
                let header = |key: &str| {
                    headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(": ")
                                .filter(|(name, _)| name.eq_ignore_ascii_case(key))
                                .map(|(_, value)| value)
                        })
                        .unwrap()
                };
                assert_eq!(header("agnt5-webhook-id"), "delivery-1");
                let timestamp = header("agnt5-webhook-timestamp").parse().unwrap();
                assert_eq!(
                    header("agnt5-webhook-signature"),
                    format!(
                        "v1={}",
                        webhook_signature(
                            &[1; 32],
                            header("agnt5-webhook-key-id"),
                            "delivery-1",
                            timestamp,
                            &expected_body
                        )
                    )
                );
                socket.write_all(format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\nretry-after: 42\r\nlocation: /events/redirected\r\n\r\n").as_bytes()).await.unwrap();
            }
        });
        for expected in [
            DeliveryResult::Retry {
                after: Duration::from_secs(42),
                status: Some(429),
            },
            DeliveryResult::Rejected {
                status: Some(302),
                reason: "destination rejected delivery",
            },
            DeliveryResult::Delivered,
        ] {
            let result = send_once(
                &format!("{origin}/events/receive"),
                &policy,
                body.clone(),
                Signature::Webhook {
                    delivery_id: "delivery-1",
                    key_id: "key-1",
                    secret: &[1; 32],
                },
            )
            .await;
            assert_eq!(result, expected);
        }
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn signature_binds_exact_body_identity_and_timestamp() {
        let expected = webhook_signature(&[1; 32], "key-1", "delivery", 123, b"{\"x\":1}");
        assert_ne!(
            expected,
            webhook_signature(&[1; 32], "key-1", "other", 123, b"{\"x\":1}")
        );
        assert_ne!(
            expected,
            webhook_signature(&[1; 32], "key-1", "delivery", 124, b"{\"x\":1}")
        );
        assert_ne!(
            expected,
            webhook_signature(&[1; 32], "key-1", "delivery", 123, b"{ \"x\":1}")
        );
        assert_ne!(
            expected,
            webhook_signature(&[1; 32], "key-2", "delivery", 123, b"{\"x\":1}")
        );
        assert_ne!(
            expected,
            webhook_signature(&[2; 32], "key-1", "delivery", 123, b"{\"x\":1}")
        );
    }
}
