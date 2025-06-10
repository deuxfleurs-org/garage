#![recursion_limit = "1024"]
//! Garage CLI, used to interact with a running Garage instance, and to launch a Garage instance

#[macro_use]
extern crate tracing;

mod admin;
mod cli;
mod repair;
mod secrets;
mod server;
#[cfg(feature = "telemetry-otlp")]
mod tracing_setup;

#[cfg(not(any(feature = "bundled-libs", feature = "system-libs")))]
compile_error!("Either bundled-libs or system-libs Cargo feature must be enabled");

#[cfg(all(feature = "bundled-libs", feature = "system-libs"))]
compile_error!("Only one of bundled-libs and system-libs Cargo features must be enabled");

#[cfg(not(any(feature = "lmdb", feature = "sqlite")))]
compile_error!("Must activate the Cargo feature for at least one DB engine: lmdb or sqlite.");

use std::net::SocketAddr;
use std::path::PathBuf;

use structopt::StructOpt;

use garage_net::util::parse_and_resolve_peer_addr;
use garage_net::NetworkKey;

use garage_util::error::*;

use garage_rpc::system::*;
use garage_rpc::*;

use garage_model::helper::error::Error as HelperError;

use admin::*;
use cli::*;
use secrets::Secrets;

#[derive(StructOpt, Debug)]
#[structopt(
	name = "garage",
	about = "S3-compatible object store for self-hosted geo-distributed deployments"
)]
struct Opt {
	/// Host to connect to for admin operations, in the format: <full-node-id>@<ip>:<port>
	#[structopt(short = "h", long = "rpc-host", env = "GARAGE_RPC_HOST")]
	pub rpc_host: Option<String>,

	#[structopt(flatten)]
	pub secrets: Secrets,

	/// Path to configuration file
	#[structopt(
		short = "c",
		long = "config",
		env = "GARAGE_CONFIG_FILE",
		default_value = "/etc/garage.toml"
	)]
	pub config_file: PathBuf,

	#[structopt(subcommand)]
	cmd: Command,
}

#[tokio::main]
async fn main() {
	// Initialize version and features info
	let features = &[
		#[cfg(feature = "k2v")]
		"k2v",
		#[cfg(feature = "lmdb")]
		"lmdb",
		#[cfg(feature = "sqlite")]
		"sqlite",
		#[cfg(feature = "consul-discovery")]
		"consul-discovery",
		#[cfg(feature = "kubernetes-discovery")]
		"kubernetes-discovery",
		#[cfg(feature = "metrics")]
		"metrics",
		#[cfg(feature = "telemetry-otlp")]
		"telemetry-otlp",
		#[cfg(feature = "bundled-libs")]
		"bundled-libs",
		#[cfg(feature = "system-libs")]
		"system-libs",
	][..];
	if let Some(git_version) = option_env!("GIT_VERSION") {
		garage_util::version::init_version(git_version);
	} else {
		garage_util::version::init_version(git_version::git_version!(
			prefix = "git:",
			cargo_prefix = "cargo:",
			fallback = "unknown"
		));
	}
	garage_util::version::init_features(features);

	let version = format!(
		"{} [features: {}]",
		garage_util::version::garage_version(),
		features.join(", ")
	);

	// Initialize panic handler that aborts on panic and shows a nice message.
	// By default, Tokio continues running normally when a task panics. We want
	// to avoid this behavior in Garage as this would risk putting the process in an
	// unknown/uncontrollable state. We prefer to exit the process and restart it
	// from scratch, so that it boots back into a fresh, known state.
	let panic_version_info = version.clone();
	std::panic::set_hook(Box::new(move |panic_info| {
		eprintln!("======== PANIC (internal Garage error) ========");
		eprintln!("{}", panic_info);
		eprintln!();
		eprintln!("Panics are internal errors that Garage is unable to handle on its own.");
		eprintln!("They can be caused by bugs in Garage's code, or by corrupted data in");
		eprintln!("the node's storage. If you feel that this error is likely to be a bug");
		eprintln!("in Garage, please report it on our issue tracker a the following address:");
		eprintln!();
		eprintln!("        https://git.deuxfleurs.fr/Deuxfleurs/garage/issues");
		eprintln!();
		eprintln!("Please include the last log messages and the the full backtrace below in");
		eprintln!("your bug report, as well as any relevant information on the context in");
		eprintln!("which Garage was running when this error occurred.");
		eprintln!();
		eprintln!("GARAGE VERSION: {}", panic_version_info);
		eprintln!();
		eprintln!("BACKTRACE:");
		eprintln!("{:?}", backtrace::Backtrace::new());
		std::process::abort();
	}));

	// Parse arguments and dispatch command line
	let opt = Opt::from_clap(&Opt::clap().version(version.as_str()).get_matches());

	// Initialize logging as well as other libraries used in Garage
	init_logging(&opt);

	sodiumoxide::init().expect("Unable to init sodiumoxide");

	let res = match opt.cmd {
		Command::Server => server::run_server(opt.config_file, opt.secrets).await,
		Command::OfflineRepair(repair_opt) => {
			repair::offline::offline_repair(opt.config_file, opt.secrets, repair_opt).await
		}
		Command::ConvertDb(conv_opt) => {
			cli::convert_db::do_conversion(conv_opt).map_err(From::from)
		}
		Command::Node(NodeOperation::NodeId(node_id_opt)) => {
			node_id_command(opt.config_file, node_id_opt.quiet)
		}
		_ => cli_command(opt).await,
	};

	if let Err(e) = res {
		eprintln!("Error: {}", e);
		std::process::exit(1);
	}
}

fn init_logging(opt: &Opt) {
	if std::env::var("RUST_LOG").is_err() {
		let default_log = match &opt.cmd {
			Command::Server => "netapp=info,garage=info",
			_ => "netapp=warn,garage=warn",
		};
		std::env::set_var("RUST_LOG", default_log)
	}

	let env_filter = tracing_subscriber::filter::EnvFilter::from_default_env();

	if std::env::var("GARAGE_LOG_TO_SYSLOG")
		.map(|x| x == "1" || x == "true")
		.unwrap_or(false)
	{
		#[cfg(feature = "syslog")]
		{
			use std::ffi::CStr;
			use syslog_tracing::{Facility, Options, Syslog};

			let syslog = Syslog::new(
				CStr::from_bytes_with_nul(b"garage\0").unwrap(),
				Options::LOG_PID | Options::LOG_PERROR,
				Facility::Daemon,
			)
			.expect("Unable to init syslog");

			tracing_subscriber::fmt()
				.with_writer(syslog)
				.with_env_filter(env_filter)
				.with_ansi(false) // disable ANSI escape sequences (colours)
				.with_file(false)
				.with_level(false)
				.without_time()
				.compact()
				.init();

			return;
		}
		#[cfg(not(feature = "syslog"))]
		{
			eprintln!("Syslog support is not enabled in this build.");
			std::process::exit(1);
		}
	}

	if std::env::var("GARAGE_LOG_TO_JOURNALD")
		.map(|x| x == "1" || x == "true")
		.unwrap_or(false)
	{
		#[cfg(feature = "journald")]
		{
			use tracing_journald::{Priority, PriorityMappings};
			use tracing_subscriber::layer::SubscriberExt;
			use tracing_subscriber::util::SubscriberInitExt;

			let registry = tracing_subscriber::registry()
				.with(tracing_subscriber::fmt::layer().with_writer(std::io::sink))
				.with(env_filter);
			match tracing_journald::layer() {
				Ok(layer) => {
					registry
						.with(layer.with_priority_mappings(PriorityMappings {
							info: Priority::Informational,
							debug: Priority::Debug,
							..PriorityMappings::new()
						}))
						.init();
				}
				Err(e) => {
					eprintln!("Couldn't connect to journald: {}.", e);
					std::process::exit(1);
				}
			}
			return;
		}
		#[cfg(not(feature = "journald"))]
		{
			eprintln!("Journald support is not enabled in this build.");
			std::process::exit(1);
		}
	}

	tracing_subscriber::fmt()
		.with_writer(std::io::stderr)
		.with_env_filter(env_filter)
		.init();
}

async fn cli_command(opt: Opt) -> Result<(), Error> {
	let config = if (opt.secrets.rpc_secret.is_none() && opt.secrets.rpc_secret_file.is_none())
		|| opt.rpc_host.is_none()
	{
		Some(garage_util::config::read_config(opt.config_file.clone())
			.err_context(format!("Unable to read configuration file {}. Configuration file is needed because -h or -s is not provided on the command line.", opt.config_file.to_string_lossy()))?)
	} else {
		None
	};

	// Find and parse network RPC secret
	let mut rpc_secret = config.as_ref().and_then(|c| c.rpc_secret.clone());
	secrets::fill_secret(
		&mut rpc_secret,
		&config.as_ref().and_then(|c| c.rpc_secret_file.clone()),
		&opt.secrets.rpc_secret,
		&opt.secrets.rpc_secret_file,
		"rpc_secret",
		true,
	)?;

	let net_key_hex_str = rpc_secret.ok_or("No RPC secret provided")?;
	let network_key = NetworkKey::from_slice(
		&hex::decode(&net_key_hex_str).err_context("Invalid RPC secret key (bad hex)")?[..],
	)
	.ok_or("Invalid RPC secret provided (wrong length)")?;

	// Generate a temporary keypair for our RPC client
	let (_pk, sk) = sodiumoxide::crypto::sign::ed25519::gen_keypair();

	let netapp = NetApp::new(GARAGE_VERSION_TAG, network_key, sk, None);

	// Find and parse the address of the target host
	let (id, addr, is_default_addr) = if let Some(h) = opt.rpc_host {
		let (id, addrs) = parse_and_resolve_peer_addr(&h).ok_or_else(|| format!("Invalid RPC remote node identifier: {}. Expected format is <full node id>@<IP or hostname>:<port>.", h))?;
		(id, addrs[0], false)
	} else {
		let node_id = garage_rpc::system::read_node_id(&config.as_ref().unwrap().metadata_dir)
			.err_context(READ_KEY_ERROR)?;
		if let Some(a) = config.as_ref().and_then(|c| c.rpc_public_addr.as_ref()) {
			use std::net::ToSocketAddrs;
			let a = a
				.to_socket_addrs()
				.ok_or_message("unable to resolve rpc_public_addr specified in config file")?
				.next()
				.ok_or_message("unable to resolve rpc_public_addr specified in config file")?;
			(node_id, a, false)
		} else {
			let default_addr = SocketAddr::new(
				"127.0.0.1".parse().unwrap(),
				config.as_ref().unwrap().rpc_bind_addr.port(),
			);
			(node_id, default_addr, true)
		}
	};

	// Connect to target host
	if let Err(e) = netapp.clone().try_connect(addr, id).await {
		if is_default_addr {
			warn!(
				"Tried to contact Garage node at default address {}, which didn't work. If that address is wrong, consider setting rpc_public_addr in your config file.",
				addr
			);
		}
		Err(e).err_context("Unable to connect to destination RPC host. Check that you are using the same value of rpc_secret as them, and that you have their correct full-length node ID (public key).")?;
	}

	let system_rpc_endpoint = netapp.endpoint::<SystemRpc, ()>(SYSTEM_RPC_PATH.into());
	let admin_rpc_endpoint = netapp.endpoint::<AdminRpc, ()>(ADMIN_RPC_PATH.into());

	match cli_command_dispatch(opt.cmd, &system_rpc_endpoint, &admin_rpc_endpoint, id).await {
		Err(HelperError::Internal(i)) => Err(Error::Message(format!("Internal error: {}", i))),
		Err(HelperError::BadRequest(b)) => Err(Error::Message(b)),
		Err(e) => Err(Error::Message(format!("{}", e))),
		Ok(x) => Ok(x),
	}
}
