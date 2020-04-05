mod error;
mod data;
mod proto;
mod rpc;
mod api;

use structopt::StructOpt;
use futures::channel::oneshot;
use tokio::sync::Mutex;
use hyper::client::Client;

use data::*;


#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
pub struct Opt {
	#[structopt(long = "api-port", default_value = "3900")]
	api_port: u16,

	#[structopt(long = "rpc-port", default_value = "3901")]
	rpc_port: u16,
}

pub struct System {
	pub opt: Opt,

	pub rpc_client: Client<hyper::client::HttpConnector, hyper::Body>,

	pub network_members: Mutex<NetworkMembers>,
}

async fn shutdown_signal(chans: Vec<oneshot::Sender<()>>) {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
	for ch in chans {
		ch.send(()).unwrap();
	}
}

async fn wait_from(chan: oneshot::Receiver<()>) -> () {
	chan.await.unwrap()
}

#[tokio::main]
async fn main() {
	let opt = Opt::from_args();
	let rpc_port = opt.rpc_port;
	let api_port = opt.api_port;

	let sys = System{
		opt,
		rpc_client: Client::new(),
		network_members: Mutex::new(NetworkMembers::default()),
	};

	let (tx1, rx1) = oneshot::channel();
	let (tx2, rx2) = oneshot::channel();

	tokio::spawn(shutdown_signal(vec![tx1, tx2]));

	let rpc_server = rpc::run_rpc_server(&sys, rpc_port, wait_from(rx1));
	let api_server = api::run_api_server(&sys, api_port, wait_from(rx2));

	let (e1, e2) = futures::join![rpc_server, api_server];

	if let Err(e) = e1 {
		eprintln!("RPC server error: {}", e)
	}

	if let Err(e) = e2 {
		eprintln!("API server error: {}", e)
	}
}
