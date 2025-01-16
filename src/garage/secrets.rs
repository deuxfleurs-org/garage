use std::path::PathBuf;

use structopt::StructOpt;

use garage_util::config::Config;
use garage_util::error::Error;

/// Structure for secret values or paths that are passed as CLI arguments or environment
/// variables, instead of in the config file.
#[derive(StructOpt, Debug, Default, Clone)]
pub struct Secrets {
	/// Skip permission check on files containing secrets
	#[cfg(unix)]
	#[structopt(
		long = "allow-world-readable-secrets",
		env = "GARAGE_ALLOW_WORLD_READABLE_SECRETS"
	)]
	pub allow_world_readable_secrets: Option<bool>,

	/// RPC secret network key, used to replace rpc_secret in config.toml when running the
	/// daemon or doing admin operations
	#[structopt(short = "s", long = "rpc-secret", env = "GARAGE_RPC_SECRET")]
	pub rpc_secret: Option<String>,

	/// RPC secret network key, used to replace rpc_secret in config.toml and rpc-secret
	/// when running the daemon or doing admin operations
	#[structopt(long = "rpc-secret-file", env = "GARAGE_RPC_SECRET_FILE")]
	pub rpc_secret_file: Option<PathBuf>,

	/// Admin API authentication token, replaces admin.admin_token in config.toml when
	/// running the Garage daemon
	#[structopt(long = "admin-token", env = "GARAGE_ADMIN_TOKEN")]
	pub admin_token: Option<String>,

	/// Admin API authentication token file path, replaces admin.admin_token in config.toml
	/// and admin-token when running the Garage daemon
	#[structopt(long = "admin-token-file", env = "GARAGE_ADMIN_TOKEN_FILE")]
	pub admin_token_file: Option<PathBuf>,

	/// Metrics API authentication token, replaces admin.metrics_token in config.toml when
	/// running the Garage daemon
	#[structopt(long = "metrics-token", env = "GARAGE_METRICS_TOKEN")]
	pub metrics_token: Option<String>,

	/// Metrics API authentication token file path, replaces admin.metrics_token in config.toml
	/// and metrics-token when running the Garage daemon
	#[structopt(long = "metrics-token-file", env = "GARAGE_METRICS_TOKEN_FILE")]
	pub metrics_token_file: Option<PathBuf>,
}

/// Single function to fill all secrets in the Config struct from their correct source (value
/// from config or CLI param or env variable or read from a file specified in config or CLI
/// param or env variable)
pub fn fill_secrets(mut config: Config, secrets: Secrets) -> Result<Config, Error> {
	let allow_world_readable = secrets
		.allow_world_readable_secrets
		.unwrap_or(config.allow_world_readable_secrets);

	fill_secret(
		&mut config.rpc_secret,
		&config.rpc_secret_file,
		&secrets.rpc_secret,
		&secrets.rpc_secret_file,
		"rpc_secret",
		allow_world_readable,
	)?;

	fill_secret(
		&mut config.admin.admin_token,
		&config.admin.admin_token_file,
		&secrets.admin_token,
		&secrets.admin_token_file,
		"admin.admin_token",
		allow_world_readable,
	)?;
	fill_secret(
		&mut config.admin.metrics_token,
		&config.admin.metrics_token_file,
		&secrets.metrics_token,
		&secrets.metrics_token_file,
		"admin.metrics_token",
		allow_world_readable,
	)?;

	Ok(config)
}

pub(crate) fn fill_secret(
	config_secret: &mut Option<String>,
	config_secret_file: &Option<PathBuf>,
	cli_secret: &Option<String>,
	cli_secret_file: &Option<PathBuf>,
	name: &'static str,
	allow_world_readable: bool,
) -> Result<(), Error> {
	let cli_value = match (&cli_secret, &cli_secret_file) {
		(Some(_), Some(_)) => {
			return Err(format!("only one of `{}` and `{}_file` can be set", name, name).into());
		}
		(Some(secret), None) => Some(secret.to_string()),
		(None, Some(file)) => Some(read_secret_file(file, allow_world_readable)?),
		(None, None) => None,
	};

	if let Some(val) = cli_value {
		if config_secret.is_some() || config_secret_file.is_some() {
			debug!("Overriding secret `{}` using value specified using CLI argument or environment variable.", name);
		}

		*config_secret = Some(val);
	} else if let Some(file_path) = &config_secret_file {
		if config_secret.is_some() {
			return Err(format!("only one of `{}` and `{}_file` can be set", name, name).into());
		}

		*config_secret = Some(read_secret_file(file_path, allow_world_readable)?);
	}

	Ok(())
}

fn read_secret_file(file_path: &PathBuf, allow_world_readable: bool) -> Result<String, Error> {
	if !allow_world_readable {
		#[cfg(unix)]
		{
			use std::os::unix::fs::MetadataExt;
			let metadata = std::fs::metadata(file_path)?;
			if metadata.mode() & 0o077 != 0 {
				return Err(format!("File {} is world-readable! (mode: 0{:o}, expected 0600)\nRefusing to start until this is fixed, or environment variable GARAGE_ALLOW_WORLD_READABLE_SECRETS is set to true.", file_path.display(), metadata.mode()).into());
			}
		}
	}

	let secret_buf = std::fs::read_to_string(file_path)?;

	// trim_end: allows for use case such as `echo "$(openssl rand -hex 32)" > somefile`.
	//           also editors sometimes add a trailing newline
	Ok(String::from(secret_buf.trim_end()))
}

#[cfg(test)]
mod tests {
	use std::fs::File;
	use std::io::Write;

	use garage_util::config::read_config;
	use garage_util::error::Error;

	use super::*;

	#[test]
	fn test_rpc_secret_file_works() -> Result<(), Error> {
		let path_secret = mktemp::Temp::new_file()?;
		let mut file_secret = File::create(path_secret.as_path())?;
		writeln!(file_secret, "foo")?;
		drop(file_secret);

		let path_config = mktemp::Temp::new_file()?;
		let mut file_config = File::create(path_config.as_path())?;
		let path_secret_path = path_secret.as_path();
		writeln!(
			file_config,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_factor = 3
			rpc_bind_addr = "[::]:3901"
			rpc_secret_file = "{}"

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#,
			path_secret_path.display()
		)?;
		drop(file_config);

		// Second configuration file, same as previous one
		// except it allows world-readable secrets.
		let path_config_allow_world_readable = mktemp::Temp::new_file()?;
		let mut file_config_allow_world_readable =
			File::create(path_config_allow_world_readable.as_path())?;
		writeln!(
			file_config_allow_world_readable,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_factor = 3
			rpc_bind_addr = "[::]:3901"
			rpc_secret_file = "{}"
			allow_world_readable_secrets = true

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#,
			path_secret_path.display()
		)?;
		drop(file_config_allow_world_readable);

		let config = read_config(path_config.to_path_buf())?;
		let config = fill_secrets(config, Secrets::default())?;
		assert_eq!("foo", config.rpc_secret.unwrap());

		// ---- Check non world-readable secrets config ----
		#[cfg(unix)]
		{
			let secrets_allow_world_readable = Secrets {
				allow_world_readable_secrets: Some(true),
				..Default::default()
			};
			let secrets_no_allow_world_readable = Secrets {
				allow_world_readable_secrets: Some(false),
				..Default::default()
			};

			use std::os::unix::fs::PermissionsExt;
			let metadata = std::fs::metadata(path_secret_path)?;
			let mut perm = metadata.permissions();
			perm.set_mode(0o660);
			std::fs::set_permissions(path_secret_path, perm)?;

			// Config file that just specifies the path
			let config = read_config(path_config.to_path_buf())?;
			assert!(fill_secrets(config, Secrets::default()).is_err());

			let config = read_config(path_config.to_path_buf())?;
			assert!(fill_secrets(config, secrets_allow_world_readable.clone()).is_ok());

			let config = read_config(path_config.to_path_buf())?;
			assert!(fill_secrets(config, secrets_no_allow_world_readable.clone()).is_err());

			// Config file that also specifies to allow world_readable_secrets
			let config = read_config(path_config_allow_world_readable.to_path_buf())?;
			assert!(fill_secrets(config, Secrets::default()).is_ok());

			let config = read_config(path_config_allow_world_readable.to_path_buf())?;
			assert!(fill_secrets(config, secrets_allow_world_readable).is_ok());

			let config = read_config(path_config_allow_world_readable.to_path_buf())?;
			assert!(fill_secrets(config, secrets_no_allow_world_readable).is_err());
		}

		// ---- Check alternative secrets specified on CLI ----

		let path_secret2 = mktemp::Temp::new_file()?;
		let mut file_secret2 = File::create(path_secret2.as_path())?;
		writeln!(file_secret2, "bar")?;
		drop(file_secret2);

		let config = read_config(path_config.to_path_buf())?;
		let config = fill_secrets(
			config,
			Secrets {
				rpc_secret: Some("baz".into()),
				..Default::default()
			},
		)?;
		assert_eq!(config.rpc_secret.as_deref(), Some("baz"));

		let config = read_config(path_config.to_path_buf())?;
		let config = fill_secrets(
			config,
			Secrets {
				rpc_secret_file: Some(path_secret2.clone()),
				..Default::default()
			},
		)?;
		assert_eq!(config.rpc_secret.as_deref(), Some("bar"));

		let config = read_config(path_config.to_path_buf())?;
		assert!(fill_secrets(
			config,
			Secrets {
				rpc_secret: Some("baz".into()),
				rpc_secret_file: Some(path_secret2.clone()),
				..Default::default()
			}
		)
		.is_err());

		drop(path_secret);
		drop(path_secret2);
		drop(path_config);
		drop(path_config_allow_world_readable);

		Ok(())
	}

	#[test]
	fn test_rcp_secret_and_rpc_secret_file_cannot_be_set_both() -> Result<(), Error> {
		let path_config = mktemp::Temp::new_file()?;
		let mut file_config = File::create(path_config.as_path())?;
		writeln!(
			file_config,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_factor = 3
			rpc_bind_addr = "[::]:3901"
			rpc_secret= "dummy"
			rpc_secret_file = "dummy"

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#
		)?;
		let config = read_config(path_config.to_path_buf())?;
		assert_eq!(
			"only one of `rpc_secret` and `rpc_secret_file` can be set",
			fill_secrets(config, Secrets::default())
				.unwrap_err()
				.to_string()
		);
		drop(path_config);
		drop(file_config);
		Ok(())
	}
}
