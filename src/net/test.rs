use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::sync::watch;

use sodiumoxide::crypto::auth;
use sodiumoxide::crypto::sign::ed25519;

use crate::netapp::*;
use crate::peering::*;
use crate::NodeID;

#[tokio::test(flavor = "current_thread")]
async fn test_with_basic_scheduler() {
	pretty_env_logger::init();
	run_test(19980).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_with_threaded_scheduler() {
	run_test(19990).await
}

async fn run_test(port_base: u16) {
	select! {
		_ = run_test_inner(port_base) => (),
		_ = tokio::time::sleep(Duration::from_secs(20)) => panic!("timeout"),
	}
}

async fn run_test_inner(port_base: u16) {
	let netid = auth::gen_key();

	let (pk1, sk1) = ed25519::gen_keypair();
	let (pk2, sk2) = ed25519::gen_keypair();
	let (pk3, sk3) = ed25519::gen_keypair();

	let addr1: SocketAddr = SocketAddr::new("127.0.0.1".parse().unwrap(), port_base);
	let addr2: SocketAddr = SocketAddr::new("127.0.0.1".parse().unwrap(), port_base + 1);
	let addr3: SocketAddr = SocketAddr::new("127.0.0.1".parse().unwrap(), port_base + 2);

	let (stop_tx, stop_rx) = watch::channel(false);

	let (thread1, _netapp1, peering1) =
		run_netapp(netid.clone(), pk1, sk1, addr1, vec![], stop_rx.clone());
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Connect second node and check it peers with everyone
	let (thread2, _netapp2, peering2) = run_netapp(
		netid.clone(),
		pk2,
		sk2,
		addr2,
		vec![(pk1, addr1)],
		stop_rx.clone(),
	);
	tokio::time::sleep(Duration::from_secs(3)).await;

	let pl1 = peering1.get_peer_list();
	println!("A pl1: {:?}", pl1);
	assert_eq!(pl1.len(), 2);

	let pl2 = peering2.get_peer_list();
	println!("A pl2: {:?}", pl2);
	assert_eq!(pl2.len(), 2);

	// Connect third node and check it peers with everyone
	let (thread3, _netapp3, peering3) =
		run_netapp(netid, pk3, sk3, addr3, vec![(pk2, addr2)], stop_rx.clone());
	tokio::time::sleep(Duration::from_secs(3)).await;

	let pl1 = peering1.get_peer_list();
	println!("B pl1: {:?}", pl1);
	assert_eq!(pl1.len(), 3);

	let pl2 = peering2.get_peer_list();
	println!("B pl2: {:?}", pl2);
	assert_eq!(pl2.len(), 3);

	let pl3 = peering3.get_peer_list();
	println!("B pl3: {:?}", pl3);
	assert_eq!(pl3.len(), 3);

	// Send stop signal and wait for everyone to finish
	stop_tx.send(true).unwrap();
	thread1.await.unwrap();
	thread2.await.unwrap();
	thread3.await.unwrap();
}

fn run_netapp(
	netid: auth::Key,
	_pk: NodeID,
	sk: ed25519::SecretKey,
	listen_addr: SocketAddr,
	bootstrap_peers: Vec<(NodeID, SocketAddr)>,
	must_exit: watch::Receiver<bool>,
) -> (
	tokio::task::JoinHandle<()>,
	Arc<NetApp>,
	Arc<PeeringManager>,
) {
	let netapp = NetApp::new(0u64, netid, sk, None);
	let peering = PeeringManager::new(netapp.clone(), bootstrap_peers, None);

	let peering2 = peering.clone();
	let netapp2 = netapp.clone();
	let fut = tokio::spawn(async move {
		tokio::join!(
			netapp2.listen(listen_addr, None, must_exit.clone()),
			peering2.run(must_exit.clone()),
		);
	});

	(fut, netapp, peering)
}
