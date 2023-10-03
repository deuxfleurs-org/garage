use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use serde::de::Error;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
pub enum UnixOrTCPSocketAddress {
	TCPSocket(SocketAddr),
	UnixSocket(PathBuf),
}

impl Display for UnixOrTCPSocketAddress {
	fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			UnixOrTCPSocketAddress::TCPSocket(address) => write!(formatter, "http://{}", address),
			UnixOrTCPSocketAddress::UnixSocket(path) => {
				write!(formatter, "http+unix://{}", path.to_string_lossy())
			}
		}
	}
}

impl<'de> Deserialize<'de> for UnixOrTCPSocketAddress {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let string = String::deserialize(deserializer)?;
		let string = string.as_str();

		if string.starts_with("/") {
			Ok(UnixOrTCPSocketAddress::UnixSocket(
				PathBuf::from_str(string).map_err(Error::custom)?,
			))
		} else {
			Ok(UnixOrTCPSocketAddress::TCPSocket(
				SocketAddr::from_str(string).map_err(Error::custom)?,
			))
		}
	}
}
