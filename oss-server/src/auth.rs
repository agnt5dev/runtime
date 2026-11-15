use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Default)]
pub struct AuthConfig {
    api_key: Option<Arc<str>>,
    jwt_secret: Option<Arc<[u8]>>,
    project_id: Arc<str>,
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
}

#[derive(Deserialize)]
struct JwtClaims {
    exp: Option<u64>,
    nbf: Option<u64>,
    project_id: Option<String>,
}

impl AuthConfig {
    pub fn new(
        api_key: Option<String>,
        jwt_secret: Option<String>,
        project_id: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.map(Arc::from),
            jwt_secret: jwt_secret.map(|secret| Arc::from(secret.into_bytes())),
            project_id: Arc::from(project_id.into()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.api_key.is_some() || self.jwt_secret.is_some()
    }

    fn authorize_headers(&self, headers: &HeaderMap) -> bool {
        if !self.enabled() {
            return true;
        }
        if let (Some(expected), Some(candidate)) = (
            self.api_key.as_deref(),
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok()),
        ) {
            if constant_time_api_key_eq(expected.as_bytes(), candidate.as_bytes()) {
                return true;
            }
        }
        let bearer = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        match (self.jwt_secret.as_deref(), bearer) {
            (Some(secret), Some(token)) => validate_hs256_jwt(token, secret, &self.project_id),
            _ => false,
        }
    }
}

pub async fn authorize(State(config): State<AuthConfig>, request: Request, next: Next) -> Response {
    if config.authorize_headers(request.headers()) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        )
            .into_response()
    }
}

fn constant_time_api_key_eq(expected: &[u8], candidate: &[u8]) -> bool {
    const CONTEXT: &[u8] = b"agnt5-oss-api-key-v1";
    let Ok(expected_mac) = HmacSha256::new_from_slice(expected) else {
        return false;
    };
    let expected_tag = expected_mac.chain_update(CONTEXT).finalize().into_bytes();
    let Ok(mut candidate_mac) = HmacSha256::new_from_slice(candidate) else {
        return false;
    };
    candidate_mac.update(CONTEXT);
    candidate_mac.verify_slice(&expected_tag).is_ok()
}

fn validate_hs256_jwt(token: &str, secret: &[u8], project_id: &str) -> bool {
    let mut parts = token.split('.');
    let (Some(header_b64), Some(claims_b64), Some(signature_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Ok(header_bytes) = URL_SAFE_NO_PAD.decode(header_b64) else {
        return false;
    };
    let Ok(header) = serde_json::from_slice::<JwtHeader>(&header_bytes) else {
        return false;
    };
    if header.alg != "HS256" {
        return false;
    }
    let Ok(signature) = URL_SAFE_NO_PAD.decode(signature_b64) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(format!("{header_b64}.{claims_b64}").as_bytes());
    if mac.verify_slice(&signature).is_err() {
        return false;
    }
    let Ok(claims_bytes) = URL_SAFE_NO_PAD.decode(claims_b64) else {
        return false;
    };
    let Ok(claims) = serde_json::from_slice::<JwtClaims>(&claims_bytes) else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    if claims.exp.is_none_or(|expiry| expiry <= now) {
        return false;
    }
    if claims.nbf.is_some_and(|not_before| now < not_before) {
        return false;
    }
    // A token must name the project it is for, so one minted for another
    // runtime's project can't be replayed here.
    claims.project_id.as_deref() == Some(project_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_comparison_is_exact() {
        assert!(constant_time_api_key_eq(b"secret", b"secret"));
        assert!(!constant_time_api_key_eq(b"secret", b"Secret"));
        assert!(!constant_time_api_key_eq(b"secret", b"secret-longer"));
    }

    fn signed_token(claims_json: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(claims_json);
        let signing_input = format!("{header}.{claims}");
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing_input}.{signature}")
    }

    fn expiry_in_a_minute() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60
    }

    #[test]
    fn jwt_requires_valid_signature_expiry_and_project() {
        let token = signed_token(&format!(
            r#"{{"exp":{},"project_id":"p1"}}"#,
            expiry_in_a_minute()
        ));

        assert!(validate_hs256_jwt(&token, b"secret", "p1"));
        assert!(!validate_hs256_jwt(&token, b"wrong", "p1"));
        assert!(!validate_hs256_jwt(&token, b"secret", "p2"));
    }

    #[test]
    fn jwt_without_a_project_is_rejected() {
        let token = signed_token(&format!(r#"{{"exp":{}}}"#, expiry_in_a_minute()));
        assert!(!validate_hs256_jwt(&token, b"secret", "p1"));
    }
}
