#[macro_use]
extern crate tracing;

pub mod api_server;
pub mod error;

mod bucket;
mod copy;
pub mod cors;
mod delete;
pub mod get;
mod lifecycle;
mod list;
mod multipart;
mod post_object;
mod put;
pub mod website;

mod encryption;
mod router;
pub mod xml;

pub fn generate_public_object_url(
	config: &garage_util::config::S3ApiConfig,
	bucket: &str,
	key: &str,
) -> String {
	fn inner(
		path_style: bool,
		public_endpoint: &http::Uri,
		bucket: &str,
		key: &str,
	) -> Option<String> {
		use http::Uri;

		if path_style {
			let mut uri = public_endpoint.clone().into_parts();
			uri.path_and_query = Some(
				http::uri::PathAndQuery::from_maybe_shared(
					format!("/{}/{}", bucket, key).into_bytes(),
				)
				.ok()?,
			);

			Some(Uri::from_parts(uri).ok()?.to_string())
		} else {
			let mut uri = public_endpoint.clone().into_parts();

			let au = uri.authority.as_ref().unwrap();
			let host = if let Some(port) = au.port() {
				format!("{}.{}:{}", bucket, au.host(), port)
			} else {
				format!("{}.{}", bucket, au.host())
			};
			uri.authority = Some(http::uri::Authority::from_maybe_shared(host.into_bytes()).ok()?);

			uri.path_and_query = Some(
				http::uri::PathAndQuery::from_maybe_shared(format!("/{}", key).into_bytes())
					.ok()?,
			);

			Some(Uri::from_parts(uri).ok()?.to_string())
		}
	}

	let key = garage_api_common::encoding::uri_encode(key, false);

	if let Some(public_endpoint) = &config.advertise_endpoint {
		let path_style = config
			.advertise_path_style
			.unwrap_or(config.root_domain.is_none());

		if let Some(res) = inner(path_style, public_endpoint, bucket, &key) {
			return res;
		}
	}

	// Fall back to older method
	if let Some(root_domain) = &config.root_domain {
		// FIXME: the returned URL uses https, but the client might want http
		format!(
			"https://{}.{}/{}",
			bucket,
			root_domain.trim_start_matches('.'),
			key
		)
	} else {
		// FIXME: what to return here??
		format!("/{}/{}", bucket, key)
	}
}
