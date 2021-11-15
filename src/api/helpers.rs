use crate::Error;
use idna::domain_to_unicode;

/// Host to bucket
///
/// Convert a host, like "bucket.garage-site.tld" to the corresponding bucket "bucket",
/// considering that ".garage-site.tld" is the "root domain". For domains not matching
/// the provided root domain, no bucket is returned
/// This behavior has been chosen to follow AWS S3 semantic.
pub fn host_to_bucket<'a>(host: &'a str, root: &str) -> Option<&'a str> {
	let root = root.trim_start_matches('.');
	let label_root = root.chars().filter(|c| c == &'.').count() + 1;
	let root = root.rsplit('.');
	let mut host = host.rsplitn(label_root + 1, '.');
	for root_part in root {
		let host_part = host.next()?;
		if root_part != host_part {
			return None;
		}
	}
	host.next()
}

/// Extract host from the authority section given by the HTTP host header
///
/// The HTTP host contains both a host and a port.
/// Extracting the port is more complex than just finding the colon (:) symbol due to IPv6
/// We do not use the collect pattern as there is no way in std rust to collect over a stack allocated value
/// check here: https://docs.rs/collect_slice/1.2.0/collect_slice/
pub fn authority_to_host(authority: &str) -> Result<String, Error> {
	let mut iter = authority.chars().enumerate();
	let (_, first_char) = iter
		.next()
		.ok_or_else(|| Error::BadRequest("Authority is empty".to_string()))?;

	let split = match first_char {
		'[' => {
			let mut iter = iter.skip_while(|(_, c)| c != &']');
			match iter.next() {
				Some((_, ']')) => iter.next(),
				_ => {
					return Err(Error::BadRequest(format!(
						"Authority {} has an illegal format",
						authority
					)))
				}
			}
		}
		_ => iter.find(|(_, c)| *c == ':'),
	};

	let authority = match split {
		Some((i, ':')) => Ok(&authority[..i]),
		None => Ok(authority),
		Some((_, _)) => Err(Error::BadRequest(format!(
			"Authority {} has an illegal format",
			authority
		))),
	};
	authority.map(|h| domain_to_unicode(h).0)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn authority_to_host_with_port() -> Result<(), Error> {
		let domain = authority_to_host("[::1]:3902")?;
		assert_eq!(domain, "[::1]");
		let domain2 = authority_to_host("garage.tld:65200")?;
		assert_eq!(domain2, "garage.tld");
		let domain3 = authority_to_host("127.0.0.1:80")?;
		assert_eq!(domain3, "127.0.0.1");
		Ok(())
	}

	#[test]
	fn authority_to_host_without_port() -> Result<(), Error> {
		let domain = authority_to_host("[::1]")?;
		assert_eq!(domain, "[::1]");
		let domain2 = authority_to_host("garage.tld")?;
		assert_eq!(domain2, "garage.tld");
		let domain3 = authority_to_host("127.0.0.1")?;
		assert_eq!(domain3, "127.0.0.1");
		assert!(authority_to_host("[").is_err());
		assert!(authority_to_host("[hello").is_err());
		Ok(())
	}

	#[test]
	fn host_to_bucket_test() {
		assert_eq!(
			host_to_bucket("john.doe.garage.tld", ".garage.tld").unwrap(),
			"john.doe"
		);

		assert_eq!(
			host_to_bucket("john.doe.garage.tld", "garage.tld").unwrap(),
			"john.doe"
		);

		assert_eq!(host_to_bucket("john.doe.com", "garage.tld"), None);

		assert_eq!(host_to_bucket("john.doe.com", ".garage.tld"), None);

		assert_eq!(host_to_bucket("garage.tld", "garage.tld"), None);

		assert_eq!(host_to_bucket("garage.tld", ".garage.tld"), None);

		assert_eq!(host_to_bucket("not-garage.tld", "garage.tld"), None);
		assert_eq!(host_to_bucket("not-garage.tld", ".garage.tld"), None);
	}
}
