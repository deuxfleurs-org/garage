use std::{fs, io};
use core::task::{Poll, Context};
use std::pin::Pin;
use std::sync::Arc;
use core::future::Future;

use futures_util::future::*;
use tokio::io::{AsyncRead, AsyncWrite};
use rustls::internal::pemfile;
use rustls::*;
use hyper::client::HttpConnector;
use hyper::client::connect::Connection;
use hyper::service::Service;
use hyper::Uri;
use hyper_rustls::MaybeHttpsStream;
use tokio_rustls::TlsConnector;
use webpki::DNSNameRef;

use crate::error::Error;

pub fn load_certs(filename: &str) -> Result<Vec<rustls::Certificate>, Error> {
	let certfile = fs::File::open(&filename)?;
	let mut reader = io::BufReader::new(certfile);

	let certs = pemfile::certs(&mut reader).map_err(|_| {
		Error::Message(format!(
			"Could not deecode certificates from file: {}",
			filename
		))
	})?;

	if certs.is_empty() {
		return Err(Error::Message(format!(
			"Invalid certificate file: {}",
			filename
		)));
	}
	Ok(certs)
}

pub fn load_private_key(filename: &str) -> Result<rustls::PrivateKey, Error> {
	let keyfile = fs::File::open(&filename)?;
	let mut reader = io::BufReader::new(keyfile);

	let keys = pemfile::rsa_private_keys(&mut reader).map_err(|_| {
		Error::Message(format!(
			"Could not decode private key from file: {}",
			filename
		))
	})?;

	if keys.len() != 1 {
		return Err(Error::Message(format!(
			"Invalid private key file: {} ({} private keys)",
			filename,
			keys.len()
		)));
	}
	Ok(keys[0].clone())
}


// ---- AWFUL COPYPASTA FROM rustls/verifier.rs
// ---- USED TO ALLOW TO VERIFY SERVER CERTIFICATE VALIDITY IN CHAIN
// ---- BUT DISREGARD HOSTNAME PARAMETER

pub struct NoHostnameCertVerifier;

type SignatureAlgorithms = &'static [&'static webpki::SignatureAlgorithm];
static SUPPORTED_SIG_ALGS: SignatureAlgorithms = &[
	&webpki::ECDSA_P256_SHA256,
	&webpki::ECDSA_P256_SHA384,
	&webpki::ECDSA_P384_SHA256,
	&webpki::ECDSA_P384_SHA384,
	&webpki::RSA_PSS_2048_8192_SHA256_LEGACY_KEY,
	&webpki::RSA_PSS_2048_8192_SHA384_LEGACY_KEY,
	&webpki::RSA_PSS_2048_8192_SHA512_LEGACY_KEY,
	&webpki::RSA_PKCS1_2048_8192_SHA256,
	&webpki::RSA_PKCS1_2048_8192_SHA384,
	&webpki::RSA_PKCS1_2048_8192_SHA512,
	&webpki::RSA_PKCS1_3072_8192_SHA384
];

impl rustls::ServerCertVerifier for NoHostnameCertVerifier {
	fn verify_server_cert(&self,
			roots: &RootCertStore,
			presented_certs: &[Certificate],
			_dns_name: webpki::DNSNameRef,
			_ocsp_response: &[u8]) -> Result<rustls::ServerCertVerified, TLSError> {

		if presented_certs.is_empty() {
			return Err(TLSError::NoCertificatesPresented);
		}

		let cert = webpki::EndEntityCert::from(&presented_certs[0].0)
			.map_err(TLSError::WebPKIError)?;

		let chain = presented_certs.iter()
			.skip(1)
			.map(|cert| cert.0.as_ref())
			.collect::<Vec<_>>();

		let trustroots: Vec<webpki::TrustAnchor> = roots.roots
			.iter()
			.map(|x| x.to_trust_anchor())
			.collect();

		let now = webpki::Time::try_from(std::time::SystemTime::now())
		        .map_err( |_ | TLSError::FailedToGetCurrentTime)?;

		cert.verify_is_valid_tls_server_cert(SUPPORTED_SIG_ALGS,
				&webpki::TLSServerTrustAnchors(&trustroots), &chain, now)
			.map_err(TLSError::WebPKIError)?;

		Ok(rustls::ServerCertVerified::assertion())
	}
}


// ---- AWFUL COPYPASTA FROM HYPER-RUSTLS connector.rs
// ---- ALWAYS USE `garage` AS HOSTNAME FOR TLS VERIFICATION

#[derive(Clone)]
pub struct HttpsConnectorFixedDnsname<T> {
	http: T,
	tls_config: Arc<rustls::ClientConfig>,
	fixed_dnsname: &'static str,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl HttpsConnectorFixedDnsname<HttpConnector> {
	pub fn new(mut tls_config: rustls::ClientConfig, fixed_dnsname: &'static str) -> Self {
		let mut http = HttpConnector::new();
		http.enforce_http(false);
		tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
		Self {
			http,
			tls_config: Arc::new(tls_config),
			fixed_dnsname,
		}
	}
}

impl<T> Service<Uri> for HttpsConnectorFixedDnsname<T>
	where
	T: Service<Uri>,
	T::Response: Connection + AsyncRead + AsyncWrite + Send + Unpin + 'static,
	T::Future: Send + 'static,
	T::Error: Into<BoxError>,
{
    type Response = MaybeHttpsStream<T::Response>;
    type Error = BoxError;

    #[allow(clippy::type_complexity)]
    type Future =
        Pin<Box<dyn Future<Output = Result<MaybeHttpsStream<T::Response>, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.http.poll_ready(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let is_https = dst.scheme_str() == Some("https");

        if !is_https {
            let connecting_future = self.http.call(dst);

            let f = async move {
                let tcp = connecting_future.await.map_err(Into::into)?;

                Ok(MaybeHttpsStream::Http(tcp))
            };
            f.boxed()
        } else {
            let cfg = self.tls_config.clone();
            let connecting_future = self.http.call(dst);

			let dnsname = DNSNameRef::try_from_ascii_str(self.fixed_dnsname)
				.expect("Invalid fixed dnsname");

            let f = async move {
                let tcp = connecting_future.await.map_err(Into::into)?;
                let connector = TlsConnector::from(cfg);
                let tls = connector
                    .connect(dnsname, tcp)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(MaybeHttpsStream::Https(tls))
            };
            f.boxed()
        }
    }
}
