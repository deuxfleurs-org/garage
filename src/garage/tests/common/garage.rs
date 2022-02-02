use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Once;

use super::ext::*;

// https://xkcd.com/221/
const DEFAULT_PORT: u16 = 49995;

static GARAGE_TEST_SECRET: &str =
	"c3ea8cb80333d04e208d136698b1a01ae370d463f0d435ab2177510b3478bf44";

#[derive(Debug, Default)]
pub struct Key {
	pub name: String,
	pub id: String,
	pub secret: String,
}

pub struct Instance {
	process: process::Child,
	pub path: PathBuf,
	pub key: Key,
	pub api_port: u16,
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

replication_mode = "1"

rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "{secret}"

[s3_api]
s3_region = "{region}"
api_bind_addr = "127.0.0.1:{api_port}"
root_domain = ".s3.garage"

[s3_web]
bind_addr = "127.0.0.1:{web_port}"
root_domain = ".web.garage"
index = "index.html"
"#,
			path = path.display(),
			secret = GARAGE_TEST_SECRET,
			region = super::REGION,
			api_port = port,
			rpc_port = port + 1,
			web_port = port + 2,
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
			.env("RUST_LOG", "garage=info,garage_api=debug")
			.spawn()
			.expect("Could not start garage");

		Instance {
			process: child,
			path,
			key: Key::default(),
			api_port: port,
		}
	}

	fn setup(&mut self) {
		use std::{thread, time::Duration};

		// Wait for node to be ready
		thread::sleep(Duration::from_secs(2));

		self.setup_layout();

		self.key = self.new_key("garage_test");
	}

	fn setup_layout(&self) {
		let node_id = self.node_id();
		let node_short_id = &node_id[..64];

		self.command()
			.args(["layout", "assign"])
			.arg(node_short_id)
			.args(["-c", "1", "-z", "unzonned"])
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

	pub fn uri(&self) -> http::Uri {
		format!("http://127.0.0.1:{api_port}", api_port = self.api_port)
			.parse()
			.expect("Could not build garage endpoint URI")
	}

	pub fn new_key(&self, name: &str) -> Key {
		let mut key = Key::default();

		let output = self
			.command()
			.args(["key", "new"])
			.args(["--name", name])
			.expect_success_output("Could not create key");
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
			name: name.to_owned(),
			..key
		}
	}
}

static mut INSTANCE: MaybeUninit<Instance> = MaybeUninit::uninit();
static INSTANCE_INIT: Once = Once::new();

#[static_init::destructor]
extern "C" fn terminate_instance() {
	if INSTANCE_INIT.is_completed() {
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

	unsafe { INSTANCE.assume_init_ref() }
}

pub fn command(config_path: &Path) -> process::Command {
	let mut command = process::Command::new(env!("CARGO_BIN_EXE_garage"));

	command.arg("-c").arg(config_path);

	command
}
