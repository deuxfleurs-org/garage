use format_table::format_table;

use chrono::Local;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::remote::*;
use crate::cli::structs::*;

impl Cli {
	pub async fn cmd_key(&self, cmd: KeyOperation) -> Result<(), Error> {
		match cmd {
			KeyOperation::List => self.cmd_list_keys().await,
			KeyOperation::Info(query) => self.cmd_key_info(query).await,
			KeyOperation::Create(query) => self.cmd_create_key(query).await,
			KeyOperation::Rename(query) => self.cmd_rename_key(query).await,
			KeyOperation::Set(opt) => self.cmd_update_key(opt).await,
			KeyOperation::Delete(query) => self.cmd_delete_key(query).await,
			KeyOperation::Allow(query) => self.cmd_allow_key(query).await,
			KeyOperation::Deny(query) => self.cmd_deny_key(query).await,
			KeyOperation::Import(query) => self.cmd_import_key(query).await,
			KeyOperation::DeleteExpired { yes } => self.cmd_delete_expired_keys(yes).await,
		}
	}

	pub async fn cmd_list_keys(&self) -> Result<(), Error> {
		let mut keys = self.api_request(ListKeysRequest).await?;

		keys.0.sort_by_key(|x| x.created);

		let mut table = vec!["ID\tCreated\tName\tExpiration".to_string()];
		for key in keys.0.iter() {
			let exp = if key.expired {
				"expired".to_string()
			} else {
				key.expiration
					.map(|x| x.with_timezone(&Local).to_string())
					.unwrap_or("never".into())
			};
			table.push(format!(
				"{}\t{}\t{}\t{}",
				key.id,
				key.created
					.map(|x| x.with_timezone(&Local).date_naive().to_string())
					.unwrap_or_default(),
				key.name,
				exp
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_key_info(&self, opt: KeyInfoOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: opt.show_secret,
			})
			.await?;

		print_key_info(&key);

		Ok(())
	}

	pub async fn cmd_create_key(&self, opt: KeyNewOpt) -> Result<(), Error> {
		let key = self
			.api_request(CreateKeyRequest(UpdateKeyRequestBody {
				name: Some(opt.name),
				expiration: parse_expires_in(&opt.expires_in)?,
				never_expires: false,
				allow: None,
				deny: None,
			}))
			.await?;

		print_key_info(&key.0);

		Ok(())
	}

	pub async fn cmd_rename_key(&self, opt: KeyRenameOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: false,
			})
			.await?;

		let new_key = self
			.api_request(UpdateKeyRequest {
				id: key.access_key_id,
				body: UpdateKeyRequestBody {
					name: Some(opt.new_name),
					expiration: None,
					never_expires: false,
					allow: None,
					deny: None,
				},
			})
			.await?;

		print_key_info(&new_key.0);

		Ok(())
	}

	pub async fn cmd_update_key(&self, opt: KeySetOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: false,
			})
			.await?;

		let new_key = self
			.api_request(UpdateKeyRequest {
				id: key.access_key_id,
				body: UpdateKeyRequestBody {
					name: None,
					expiration: parse_expires_in(&opt.expires_in)?,
					never_expires: opt.never_expires,
					allow: None,
					deny: None,
				},
			})
			.await?;

		print_key_info(&new_key.0);

		Ok(())
	}

	pub async fn cmd_delete_key(&self, opt: KeyDeleteOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: false,
			})
			.await?;

		if !opt.yes {
			println!("About to delete key {}...", key.access_key_id);
			return Err(Error::Message(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}

		self.api_request(DeleteKeyRequest {
			id: key.access_key_id.clone(),
		})
		.await?;

		println!("Access key {} has been deleted.", key.access_key_id);

		Ok(())
	}

	pub async fn cmd_allow_key(&self, opt: KeyPermOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: false,
			})
			.await?;

		let new_key = self
			.api_request(UpdateKeyRequest {
				id: key.access_key_id,
				body: UpdateKeyRequestBody {
					name: None,
					expiration: None,
					never_expires: false,
					allow: Some(KeyPerm {
						create_bucket: opt.create_bucket,
					}),
					deny: None,
				},
			})
			.await?;

		print_key_info(&new_key.0);

		Ok(())
	}

	pub async fn cmd_deny_key(&self, opt: KeyPermOpt) -> Result<(), Error> {
		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern),
				show_secret_key: false,
			})
			.await?;

		let new_key = self
			.api_request(UpdateKeyRequest {
				id: key.access_key_id,
				body: UpdateKeyRequestBody {
					name: None,
					expiration: None,
					never_expires: false,
					allow: None,
					deny: Some(KeyPerm {
						create_bucket: opt.create_bucket,
					}),
				},
			})
			.await?;

		print_key_info(&new_key.0);

		Ok(())
	}

	pub async fn cmd_import_key(&self, opt: KeyImportOpt) -> Result<(), Error> {
		if !opt.yes {
			return Err(Error::Message("This command is intended to re-import keys that were previously generated by Garage. If you want to create a new key, use `garage key new` instead. Add the --yes flag if you really want to re-import a key.".to_string()));
		}

		let new_key = self
			.api_request(ImportKeyRequest {
				name: Some(opt.name),
				access_key_id: opt.key_id,
				secret_access_key: opt.secret_key,
			})
			.await?;

		print_key_info(&new_key.0);

		Ok(())
	}

	pub async fn cmd_delete_expired_keys(&self, yes: bool) -> Result<(), Error> {
		let mut list = self.api_request(ListKeysRequest).await?.0;

		list.retain(|key| key.expired);

		if !yes {
			return Err(Error::Message(format!(
				"This would delete {} access keys, add the --yes flag to proceed.",
				list.len(),
			)));
		}

		for key in list.iter() {
			let id = key.id.clone();
			println!("Deleting access key `{}` ({})", key.name, id);
			self.api_request(DeleteKeyRequest { id }).await?;
		}

		println!("{} access keys have been deleted.", list.len());

		Ok(())
	}
}

fn print_key_info(key: &GetKeyInfoResponse) {
	println!("==== ACCESS KEY INFORMATION ====");

	let mut table = vec![
		format!("Key ID:\t{}", key.access_key_id),
		format!("Key name:\t{}", key.name),
		format!(
			"Secret key:\t{}",
			key.secret_access_key.as_deref().unwrap_or("(redacted)")
		),
	];

	if let Some(c) = key.created {
		table.push(format!("Created:\t{}", c.with_timezone(&Local)));
	}

	table.extend([
		format!(
			"Validity:\t{}",
			key.expired.then_some("EXPIRED").unwrap_or("valid")
		),
		format!(
			"Expiration:\t{}",
			key.expiration
				.map(|x| x.with_timezone(&Local).to_string())
				.unwrap_or("never".into())
		),
		String::new(),
		format!("Can create buckets:\t{}", key.permissions.create_bucket),
	]);
	format_table(table);

	println!("");
	println!("==== BUCKETS FOR THIS KEY ====");
	let mut bucket_info = vec!["Permissions\tID\tGlobal aliases\tLocal aliases".to_string()];
	bucket_info.extend(key.buckets.iter().map(|bucket| {
		let rflag = if bucket.permissions.read { "R" } else { " " };
		let wflag = if bucket.permissions.write { "W" } else { " " };
		let oflag = if bucket.permissions.owner { "O" } else { " " };
		format!(
			"{}{}{}\t{:.16}\t{}\t{}",
			rflag,
			wflag,
			oflag,
			bucket.id,
			table_list_abbr(&bucket.global_aliases),
			bucket.local_aliases.join(","),
		)
	}));

	format_table(bucket_info);
}
