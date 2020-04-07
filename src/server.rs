use std::io::{Read, Write};
use std::sync::Arc;
use std::net::SocketAddr;
use std::path::PathBuf;
use futures::channel::oneshot;
use serde::Deserialize;
use rand::Rng;

use crate::data::UUID;
use crate::error::Error;
use crate::membership::System;
use crate::api_server;
use crate::rpc_server;

#[derive(Deserialize, Debug)]
pub struct Config {
	pub datacenter: String,

	pub metadata_dir: PathBuf,
	pub data_dir: PathBuf,

	pub api_port: u16,
	pub rpc_port: u16,

	pub bootstrap_peers: Vec<SocketAddr>,
}

fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;
	
	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}

fn gen_node_id(metadata_dir: &PathBuf) -> Result<UUID, Error> {
	let mut id_file = metadata_dir.clone();
	id_file.push("node_id");
	if id_file.as_path().exists() {
		let mut f = std::fs::File::open(id_file.as_path())?;
		let mut d = vec![];
		f.read_to_end(&mut d)?;
		if d.len() != 32 {
			return Err(Error::Message(format!("Corrupt node_id file")))
		}

		let mut id = [0u8; 32];
		id.copy_from_slice(&d[..]);
		Ok(id)
	} else {
		let id = rand::thread_rng().gen::<UUID>();

		let mut f = std::fs::File::create(id_file.as_path())?;
		f.write_all(&id[..])?;
		Ok(id)
	}
}

async fn shutdown_signal(chans: Vec<oneshot::Sender<()>>) {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
	println!("Received CTRL+C, shutting down.");
	for ch in chans {
		ch.send(()).unwrap();
	}
}

async fn wait_from(chan: oneshot::Receiver<()>) -> () {
	chan.await.unwrap()
}

pub async fn run_server(config_file: PathBuf) -> Result<(), Error> {
	let config = read_config(config_file)
		.expect("Unable to read config file");

	let id = gen_node_id(&config.metadata_dir)
		.expect("Unable to read or generate node ID");
	println!("Node ID: {}", hex::encode(id));

	let sys = Arc::new(System::new(config, id));

	let (tx1, rx1) = oneshot::channel();
	let (tx2, rx2) = oneshot::channel();

	let rpc_server = rpc_server::run_rpc_server(sys.clone(), wait_from(rx1));
	let api_server = api_server::run_api_server(sys.clone(), wait_from(rx2));

	tokio::spawn(shutdown_signal(vec![tx1, tx2]));
	tokio::spawn(sys.bootstrap());

	futures::try_join!(rpc_server, api_server)?;
	Ok(())
}
