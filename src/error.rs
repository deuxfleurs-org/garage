use err_derive::Error;
use std::io;

#[derive(Debug, Error)]
pub enum Error {
	#[error(display = "IO error")]
	Io(#[error(source)] io::Error),

	#[error(display = "Hyper error")]
	Hyper(#[error(source)] hyper::Error),

	#[error(display = "Messagepack encode error")]
	RMPEncode(#[error(source)] rmp_serde::encode::Error),
	#[error(display = "Messagepack decode error")]
	RMPDecode(#[error(source)] rmp_serde::decode::Error),

	#[error(display = "")]
	Msg(String),
}
