use std::{fs, io};

use rustls::internal::pemfile;

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
