//! Secrets used by runtime delivery must never appear in public run metadata.
pub fn is_public_key(key: &str) -> bool {
    !key.eq_ignore_ascii_case("workerless_signing_secret")
        && !key.eq_ignore_ascii_case("workerless_invoke_headers")
}
