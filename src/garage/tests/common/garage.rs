use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Once;

use super::ext::*;

// https://xkcd.com/221/
pub const DEFAULT_PORT: u16 = 49995;

static GARAGE_TEST_SECRET: &str =
	"c3ea8cb80333d04e208d136698b1a01ae370d463f0d435ab2177510b3478bf44";

#[derive(Debug, Default, Clone)]
pub struct Key {
	pub name: Option<String>,
	pub id: String,
	pub secret: String,
}

pub struct Instance {
	process: process::Child,
	pub path: PathBuf,
	pub default_key: Key,
	pub s3_port: u16,
	pub k2v_port: u16,
	pub web_port: u16,
	pub admin_port: u16,
}

impl Instance {
	fn new() -> Instance {
		use std::{env, fs};

		let port = env::var("GARAGE_TEST_INTEGRATION_PORT")
			.map(|value| value.parse().expect("Invalid port provided"))
			.ok()
			.unwrap_or(DEFAULT_PORT);

		let path = env::var("GARAGE_TEST_INTEGRATION_PATH")
			.map(PathBuf::from)
			.ok()
			.unwrap_or_else(|| env::temp_dir().join(format!("garage-integ-test-{}", port)));

		// Clean test runtime directory
		if path.exists() {
			fs::remove_dir_all(&path).expect("Could not clean test runtime directory");
		}
		fs::create_dir(&path).expect("Could not create test runtime directory");

		let config = format!(
			r#"
metadata_dir = "{path}/meta"
data_dir = "{path}/data"
db_engine = "lmdb"

replication_factor = 1

rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "{secret}"

[s3_api]
s3_region = "{region}"
api_bind_addr = "127.0.0.1:{s3_port}"
root_domain = ".s3.garage"

[k2v_api]
api_bind_addr = "127.0.0.1:{k2v_port}"

[s3_web]
bind_addr = "127.0.0.1:{web_port}"
root_domain = ".web.garage"
index = "index.html"

[admin]
api_bind_addr = "127.0.0.1:{admin_port}"
"#,
			path = path.display(),
			secret = GARAGE_TEST_SECRET,
			region = super::REGION,
			s3_port = port,
			k2v_port = port + 1,
			rpc_port = port + 2,
			web_port = port + 3,
			admin_port = port + 4,
		);
		fs::write(path.join("config.toml"), config).expect("Could not write garage config file");

		let stdout =
			fs::File::create(path.join("stdout.log")).expect("Could not create stdout logfile");
		let stderr =
			fs::File::create(path.join("stderr.log")).expect("Could not create stderr logfile");

		let child = command(&path.join("config.toml"))
			.arg("server")
			.stdout(stdout)
			.stderr(stderr)
			.env("RUST_LOG", "garage=debug,garage_api=trace")
			.spawn()
			.expect("Could not start garage");

		Instance {
			process: child,
			path,
			default_key: Key::default(),
			s3_port: port,
			k2v_port: port + 1,
			web_port: port + 3,
			admin_port: port + 4,
		}
	}

	fn setup(&mut self) {
		self.wait_for_boot();
		self.setup_layout();
		self.default_key = self.key(Some("garage_test"));
	}

	fn wait_for_boot(&mut self) {
		use std::{thread, time::Duration};

		// 60 * 2 seconds = 120 seconds = 2min
		for _ in 0..60 {
			let termination = self
				.command()
				.args(["status"])
				.quiet()
				.status()
				.expect("Unable to run command");
			if termination.success() {
				break;
			}
			thread::sleep(Duration::from_secs(2));
		}
	}

	fn setup_layout(&self) {
		let node_id = self.node_id();
		let node_short_id = &node_id[..64];

		self.command()
			.args(["layout", "assign"])
			.arg(node_short_id)
			.args(["-c", "1G", "-z", "unzonned"])
			.quiet()
			.expect_success_status("Could not assign garage node layout");
		self.command()
			.args(["layout", "apply"])
			.args(["--version", "1"])
			.quiet()
			.expect_success_status("Could not apply garage node layout");
	}

	fn terminate(&mut self) {
		// TODO: Terminate "gracefully" the process with SIGTERM instead of directly SIGKILL it.
		self.process
			.kill()
			.expect("Could not terminate garage process");
	}

	pub fn command(&self) -> process::Command {
		command(&self.path.join("config.toml"))
	}

	pub fn node_id(&self) -> String {
		let output = self
			.command()
			.args(["node", "id"])
			.expect_success_output("Could not get node ID");
		String::from_utf8(output.stdout).unwrap()
	}

	pub fn s3_uri(&self) -> http::Uri {
		format!("http://127.0.0.1:{s3_port}", s3_port = self.s3_port)
			.parse()
			.expect("Could not build garage endpoint URI")
	}

	pub fn k2v_uri(&self) -> http::Uri {
		format!("http://127.0.0.1:{k2v_port}", k2v_port = self.k2v_port)
			.parse()
			.expect("Could not build garage endpoint URI")
	}

	pub fn key(&self, maybe_name: Option<&str>) -> Key {
		let mut key = Key::default();

		let mut cmd = self.command();
		let base = cmd.args(["key", "create"]);
		let with_name = match maybe_name {
			Some(name) => base.args([name]),
			None => base,
		};

		let output = with_name.expect_success_output("Could not create key");
		let stdout = String::from_utf8(output.stdout).unwrap();

		for line in stdout.lines() {
			if let Some(key_id) = line.strip_prefix("Key ID: ") {
				key.id = key_id.to_owned();
				continue;
			}
			if let Some(key_secret) = line.strip_prefix("Secret key: ") {
				key.secret = key_secret.to_owned();
				continue;
			}
		}
		assert!(!key.id.is_empty(), "Invalid key: Key ID is empty");
		assert!(!key.secret.is_empty(), "Invalid key: Key secret is empty");

		Key {
			name: maybe_name.map(String::from),
			..key
		}
	}
}

static mut INSTANCE: MaybeUninit<Instance> = MaybeUninit::uninit();
static INSTANCE_INIT: Once = Once::new();

#[static_init::destructor]
extern "C" fn terminate_instance() {
	if INSTANCE_INIT.is_completed() {
		// This block is sound as it depends on `INSTANCE_INIT` being completed, meaning `INSTANCE`
		// is actually initialized.
		unsafe {
			INSTANCE.assume_init_mut().terminate();
		}
	}
}

pub fn instance() -> &'static Instance {
	INSTANCE_INIT.call_once(|| unsafe {
		let mut instance = Instance::new();
		instance.setup();

		INSTANCE.write(instance);
	});

	// This block is sound as it depends on `INSTANCE_INIT` being completed by calling `call_once` (blocking),
	// meaning `INSTANCE` is actually initialized.
	unsafe { INSTANCE.assume_init_ref() }
}

pub fn command(config_path: &Path) -> process::Command {
	use std::env;

	let mut command = process::Command::new(
		env::var("GARAGE_TEST_INTEGRATION_EXE")
			.unwrap_or_else(|_| env!("CARGO_BIN_EXE_garage").to_owned()),
	);

	command.arg("-c").arg(config_path);

	command
}
