/// Installs aws-lc-rs as the process-wide rustls crypto provider, if none is installed yet.
/// Needed because dependencies enable both rustls backends, so rustls cannot pick one itself.
pub(crate) fn ensure_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
