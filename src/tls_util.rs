use core::future::Future;
use core::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;
use std::{fs, io};

use futures_util::future::*;
use hyper::client::connect::Connection;
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Uri;
use hyper_rustls::MaybeHttpsStream;
use rustls::internal::pemfile;
use tokio::io::{AsyncRead, AsyncWrite};
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

			let dnsname =
				DNSNameRef::try_from_ascii_str(self.fixed_dnsname).expect("Invalid fixed dnsname");

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
