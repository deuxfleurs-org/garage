use http::{HeaderMap, HeaderValue};
use std::net::IpAddr;
use std::str::FromStr;

use crate::error::{Error, OkOrMessage};

pub fn handle_forwarded_for_headers(headers: &HeaderMap<HeaderValue>) -> Result<String, Error> {
	let forwarded_for_header = headers
		.get("x-forwarded-for")
		.ok_or_message("X-Forwarded-For header not provided")?;

	let forwarded_for_ip_str = forwarded_for_header
		.to_str()
		.ok_or_message("Error parsing X-Forwarded-For header")?;

	let client_ip = IpAddr::from_str(forwarded_for_ip_str)
		.ok_or_message("Valid IP address not found in X-Forwarded-For header")?;

	Ok(client_ip.to_string())
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_handle_forwarded_for_headers_ipv4_client() {
		let mut test_headers = HeaderMap::new();
		test_headers.insert("X-Forwarded-For", "192.0.2.100".parse().unwrap());

		if let Ok(forwarded_ip) = handle_forwarded_for_headers(&test_headers) {
			assert_eq!(forwarded_ip, "192.0.2.100");
		}
	}

	#[test]
	fn test_handle_forwarded_for_headers_ipv6_client() {
		let mut test_headers = HeaderMap::new();
		test_headers.insert("X-Forwarded-For", "2001:db8::f00d:cafe".parse().unwrap());

		if let Ok(forwarded_ip) = handle_forwarded_for_headers(&test_headers) {
			assert_eq!(forwarded_ip, "2001:db8::f00d:cafe");
		}
	}

	#[test]
	fn test_handle_forwarded_for_headers_invalid_ip() {
		let mut test_headers = HeaderMap::new();
		test_headers.insert("X-Forwarded-For", "www.example.com".parse().unwrap());

		let result = handle_forwarded_for_headers(&test_headers);
		assert!(result.is_err());
	}

	#[test]
	fn test_handle_forwarded_for_headers_missing() {
		let mut test_headers = HeaderMap::new();
		test_headers.insert("Host", "www.deuxfleurs.fr".parse().unwrap());

		let result = handle_forwarded_for_headers(&test_headers);
		assert!(result.is_err());
	}
}
