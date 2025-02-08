use std::net::SocketAddr;

use log::info;
use serde::Serialize;

use tokio::sync::watch;

use crate::netapp::*;

/// Utility function: encodes any serializable value in MessagePack binary format
/// using the RMP library.
///
/// Field names and variant names are included in the serialization.
/// This is used internally by the netapp communication protocol.
pub fn rmp_to_vec_all_named<T>(val: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
where
	T: Serialize + ?Sized,
{
	let mut wr = Vec::with_capacity(128);
	let mut se = rmp_serde::Serializer::new(&mut wr).with_struct_map();
	val.serialize(&mut se)?;
	Ok(wr)
}

/// This async function returns only when a true signal was received
/// from a watcher that tells us when to exit.
///
/// Useful in a select statement to interrupt another
/// future:
/// ```ignore
/// select!(
///     _ = a_long_task() => Success,
///     _ = await_exit(must_exit) => Interrupted,
/// )
/// ```
pub async fn await_exit(mut must_exit: watch::Receiver<bool>) {
	while !*must_exit.borrow_and_update() {
		if must_exit.changed().await.is_err() {
			break;
		}
	}
}

/// Creates a watch that contains `false`, and that changes
/// to `true` when a Ctrl+C signal is received.
pub fn watch_ctrl_c() -> watch::Receiver<bool> {
	let (send_cancel, watch_cancel) = watch::channel(false);
	tokio::spawn(async move {
		tokio::signal::ctrl_c()
			.await
			.expect("failed to install CTRL+C signal handler");
		info!("Received CTRL+C, shutting down.");
		send_cancel.send(true).unwrap();
	});
	watch_cancel
}

/// Parse a peer's address including public key, written in the format:
/// `<public key hex>@<ip>:<port>`
pub fn parse_peer_addr(peer: &str) -> Option<(NodeID, SocketAddr)> {
	let delim = peer.find('@')?;
	let (key, ip) = peer.split_at(delim);
	let pubkey = NodeID::from_slice(&hex::decode(key).ok()?)?;
	let ip = ip[1..].parse::<SocketAddr>().ok()?;
	Some((pubkey, ip))
}

/// Parse and resolve a peer's address including public key, written in the format:
/// `<public key hex>@<ip or hostname>:<port>`
pub fn parse_and_resolve_peer_addr(peer: &str) -> Option<(NodeID, Vec<SocketAddr>)> {
	use std::net::ToSocketAddrs;

	let delim = peer.find('@')?;
	let (key, host) = peer.split_at(delim);
	let pubkey = NodeID::from_slice(&hex::decode(key).ok()?)?;
	let hosts = host[1..].to_socket_addrs().ok()?.collect::<Vec<_>>();
	if hosts.is_empty() {
		return None;
	}
	Some((pubkey, hosts))
}

/// async version of parse_and_resolve_peer_addr
pub async fn parse_and_resolve_peer_addr_async(peer: &str) -> Option<(NodeID, Vec<SocketAddr>)> {
	let delim = peer.find('@')?;
	let (key, host) = peer.split_at(delim);
	let pubkey = NodeID::from_slice(&hex::decode(key).ok()?)?;
	let hosts = tokio::net::lookup_host(&host[1..])
		.await
		.ok()?
		.collect::<Vec<_>>();
	if hosts.is_empty() {
		return None;
	}
	Some((pubkey, hosts))
}
