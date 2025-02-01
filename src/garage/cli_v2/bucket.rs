//use bytesize::ByteSize;
use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli as cli_v1;
use crate::cli::structs::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn cmd_bucket(&self, cmd: BucketOperation) -> Result<(), Error> {
		match cmd {
			BucketOperation::List => self.cmd_list_buckets().await,
			BucketOperation::Info(query) => self.cmd_bucket_info(query).await,
			BucketOperation::Create(query) => self.cmd_create_bucket(query).await,
			BucketOperation::Delete(query) => self.cmd_delete_bucket(query).await,
			BucketOperation::Alias(query) => self.cmd_alias_bucket(query).await,
			BucketOperation::Unalias(query) => self.cmd_unalias_bucket(query).await,
			BucketOperation::Allow(query) => self.cmd_bucket_allow(query).await,
			BucketOperation::Deny(query) => self.cmd_bucket_deny(query).await,
			BucketOperation::Website(query) => self.cmd_bucket_website(query).await,
			BucketOperation::SetQuotas(query) => self.cmd_bucket_set_quotas(query).await,

			// TODO
			x => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::BucketOperation(x),
			)
			.await
			.ok_or_message("old error"),
		}
	}

	pub async fn cmd_list_buckets(&self) -> Result<(), Error> {
		let buckets = self.api_request(ListBucketsRequest).await?;

		println!("List of buckets:");

		let mut table = vec![];
		for bucket in buckets.0.iter() {
			let local_aliases_n = match &bucket.local_aliases[..] {
				[] => "".into(),
				[alias] => format!("{}:{}", alias.access_key_id, alias.alias),
				s => format!("[{} local aliases]", s.len()),
			};

			table.push(format!(
				"\t{}\t{}\t{}",
				bucket.global_aliases.join(","),
				local_aliases_n,
				bucket.id,
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_bucket_info(&self, opt: BucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.name),
			})
			.await?;

		println!("Bucket: {}", bucket.id);

		let size = bytesize::ByteSize::b(bucket.bytes as u64);
		println!(
			"\nSize: {} ({})",
			size.to_string_as(true),
			size.to_string_as(false)
		);
		println!("Objects: {}", bucket.objects);
		println!(
			"Unfinished uploads (multipart and non-multipart): {}",
			bucket.unfinished_uploads,
		);
		println!(
			"Unfinished multipart uploads: {}",
			bucket.unfinished_multipart_uploads
		);
		let mpu_size = bytesize::ByteSize::b(bucket.unfinished_multipart_uploads as u64);
		println!(
			"Size of unfinished multipart uploads: {} ({})",
			mpu_size.to_string_as(true),
			mpu_size.to_string_as(false),
		);

		println!("\nWebsite access: {}", bucket.website_access);

		if bucket.quotas.max_size.is_some() || bucket.quotas.max_objects.is_some() {
			println!("\nQuotas:");
			if let Some(ms) = bucket.quotas.max_size {
				let ms = bytesize::ByteSize::b(ms);
				println!(
					" maximum size: {} ({})",
					ms.to_string_as(true),
					ms.to_string_as(false)
				);
			}
			if let Some(mo) = bucket.quotas.max_objects {
				println!(" maximum number of objects: {}", mo);
			}
		}

		println!("\nGlobal aliases:");
		for alias in bucket.global_aliases {
			println!("  {}", alias);
		}

		println!("\nKey-specific aliases:");
		let mut table = vec![];
		for key in bucket.keys.iter() {
			for alias in key.bucket_local_aliases.iter() {
				table.push(format!("\t{} ({})\t{}", key.access_key_id, key.name, alias));
			}
		}
		format_table(table);

		println!("\nAuthorized keys:");
		let mut table = vec![];
		for key in bucket.keys.iter() {
			if !(key.permissions.read || key.permissions.write || key.permissions.owner) {
				continue;
			}
			let rflag = if key.permissions.read { "R" } else { " " };
			let wflag = if key.permissions.write { "W" } else { " " };
			let oflag = if key.permissions.owner { "O" } else { " " };
			table.push(format!(
				"\t{}{}{}\t{}\t{}",
				rflag, wflag, oflag, key.access_key_id, key.name
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_create_bucket(&self, opt: BucketOpt) -> Result<(), Error> {
		self.api_request(CreateBucketRequest {
			global_alias: Some(opt.name.clone()),
			local_alias: None,
		})
		.await?;

		println!("Bucket {} was created.", opt.name);

		Ok(())
	}

	pub async fn cmd_delete_bucket(&self, opt: DeleteBucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.name.clone()),
			})
			.await?;

		// CLI-only checks: the bucket must not have other aliases
		if bucket
			.global_aliases
			.iter()
			.find(|a| **a != opt.name)
			.is_some()
		{
			return Err(Error::Message(format!("Bucket {} still has other global aliases. Use `bucket unalias` to delete them one by one.", opt.name)));
		}

		if bucket
			.keys
			.iter()
			.any(|k| !k.bucket_local_aliases.is_empty())
		{
			return Err(Error::Message(format!("Bucket {} still has other local aliases. Use `bucket unalias` to delete them one by one.", opt.name)));
		}

		if !opt.yes {
			println!("About to delete bucket {}.", bucket.id);
			return Err(Error::Message(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}

		self.api_request(DeleteBucketRequest {
			id: bucket.id.clone(),
		})
		.await?;

		println!("Bucket {} has been deleted.", bucket.id);

		Ok(())
	}

	pub async fn cmd_alias_bucket(&self, opt: AliasBucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.existing_bucket.clone()),
			})
			.await?;

		if let Some(key_pat) = &opt.local {
			let key = self
				.api_request(GetKeyInfoRequest {
					search: Some(key_pat.clone()),
					id: None,
					show_secret_key: false,
				})
				.await?;

			self.api_request(AddBucketAliasRequest {
				bucket_id: bucket.id.clone(),
				alias: BucketAliasEnum::Local {
					local_alias: opt.new_name.clone(),
					access_key_id: key.access_key_id.clone(),
				},
			})
			.await?;

			println!(
				"Alias {} now points to bucket {:.16} in namespace of key {}",
				opt.new_name, bucket.id, key.access_key_id
			)
		} else {
			self.api_request(AddBucketAliasRequest {
				bucket_id: bucket.id.clone(),
				alias: BucketAliasEnum::Global {
					global_alias: opt.new_name.clone(),
				},
			})
			.await?;

			println!(
				"Alias {} now points to bucket {:.16}",
				opt.new_name, bucket.id
			)
		}

		Ok(())
	}

	pub async fn cmd_unalias_bucket(&self, opt: UnaliasBucketOpt) -> Result<(), Error> {
		if let Some(key_pat) = &opt.local {
			let key = self
				.api_request(GetKeyInfoRequest {
					search: Some(key_pat.clone()),
					id: None,
					show_secret_key: false,
				})
				.await?;

			let bucket = key
				.buckets
				.iter()
				.find(|x| x.local_aliases.contains(&opt.name))
				.ok_or_message(format!(
					"No bucket called {} in namespace of key {}",
					opt.name, key.access_key_id
				))?;

			self.api_request(RemoveBucketAliasRequest {
				bucket_id: bucket.id.clone(),
				alias: BucketAliasEnum::Local {
					access_key_id: key.access_key_id.clone(),
					local_alias: opt.name.clone(),
				},
			})
			.await?;

			println!(
				"Alias {} no longer points to bucket {:.16} in namespace of key {}",
				&opt.name, bucket.id, key.access_key_id
			)
		} else {
			let bucket = self
				.api_request(GetBucketInfoRequest {
					id: None,
					global_alias: Some(opt.name.clone()),
					search: None,
				})
				.await?;

			self.api_request(RemoveBucketAliasRequest {
				bucket_id: bucket.id.clone(),
				alias: BucketAliasEnum::Global {
					global_alias: opt.name.clone(),
				},
			})
			.await?;

			println!(
				"Alias {} no longer points to bucket {:.16}",
				opt.name, bucket.id
			)
		}

		Ok(())
	}

	pub async fn cmd_bucket_allow(&self, opt: PermBucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.bucket.clone()),
			})
			.await?;

		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern.clone()),
				show_secret_key: false,
			})
			.await?;

		self.api_request(AllowBucketKeyRequest(BucketKeyPermChangeRequest {
			bucket_id: bucket.id.clone(),
			access_key_id: key.access_key_id.clone(),
			permissions: ApiBucketKeyPerm {
				read: opt.read,
				write: opt.write,
				owner: opt.owner,
			},
		}))
		.await?;

		let new_bucket = self
			.api_request(GetBucketInfoRequest {
				id: Some(bucket.id),
				global_alias: None,
				search: None,
			})
			.await?;

		if let Some(new_key) = new_bucket
			.keys
			.iter()
			.find(|k| k.access_key_id == key.access_key_id)
		{
			println!(
				"New permissions for key {} on bucket {:.16}:\n  read {}\n  write {}\n  owner {}",
				key.access_key_id,
				new_bucket.id,
				new_key.permissions.read,
				new_key.permissions.write,
				new_key.permissions.owner
			);
		} else {
			println!(
				"Access key {} has no permissions on bucket {:.16}",
				key.access_key_id, new_bucket.id
			);
		}

		Ok(())
	}

	pub async fn cmd_bucket_deny(&self, opt: PermBucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.bucket.clone()),
			})
			.await?;

		let key = self
			.api_request(GetKeyInfoRequest {
				id: None,
				search: Some(opt.key_pattern.clone()),
				show_secret_key: false,
			})
			.await?;

		self.api_request(DenyBucketKeyRequest(BucketKeyPermChangeRequest {
			bucket_id: bucket.id.clone(),
			access_key_id: key.access_key_id.clone(),
			permissions: ApiBucketKeyPerm {
				read: opt.read,
				write: opt.write,
				owner: opt.owner,
			},
		}))
		.await?;

		let new_bucket = self
			.api_request(GetBucketInfoRequest {
				id: Some(bucket.id),
				global_alias: None,
				search: None,
			})
			.await?;

		if let Some(new_key) = new_bucket
			.keys
			.iter()
			.find(|k| k.access_key_id == key.access_key_id)
		{
			println!(
				"New permissions for key {} on bucket {:.16}:\n  read {}\n  write {}\n  owner {}",
				key.access_key_id,
				new_bucket.id,
				new_key.permissions.read,
				new_key.permissions.write,
				new_key.permissions.owner
			);
		} else {
			println!(
				"Access key {} no longer has permissions on bucket {:.16}",
				key.access_key_id, new_bucket.id
			);
		}

		Ok(())
	}

	pub async fn cmd_bucket_website(&self, opt: WebsiteOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.bucket.clone()),
			})
			.await?;

		if !(opt.allow ^ opt.deny) {
			return Err(Error::Message(
				"You must specify exactly one flag, either --allow or --deny".to_string(),
			));
		}

		let wa = if opt.allow {
			UpdateBucketWebsiteAccess {
				enabled: true,
				index_document: Some(opt.index_document.clone()),
				error_document: opt
					.error_document
					.or(bucket.website_config.and_then(|x| x.error_document.clone())),
			}
		} else {
			UpdateBucketWebsiteAccess {
				enabled: false,
				index_document: None,
				error_document: None,
			}
		};

		self.api_request(UpdateBucketRequest {
			id: bucket.id,
			body: UpdateBucketRequestBody {
				website_access: Some(wa),
				quotas: None,
			},
		})
		.await?;

		if opt.allow {
			println!("Website access allowed for {}", &opt.bucket);
		} else {
			println!("Website access denied for {}", &opt.bucket);
		}

		Ok(())
	}

	pub async fn cmd_bucket_set_quotas(&self, opt: SetQuotasOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.bucket.clone()),
			})
			.await?;

		if opt.max_size.is_none() && opt.max_objects.is_none() {
			return Err(Error::Message(
				"You must specify either --max-size or --max-objects (or both) for this command to do something.".to_string(),
			));
		}

		let new_quotas = ApiBucketQuotas {
			max_size: match opt.max_size.as_deref() {
				Some("none") => None,
				Some(v) => Some(
					v.parse::<bytesize::ByteSize>()
						.ok_or_message(format!("Invalid size specified: {}", v))?
						.as_u64(),
				),
				None => bucket.quotas.max_size,
			},
			max_objects: match opt.max_objects.as_deref() {
				Some("none") => None,
				Some(v) => Some(
					v.parse::<u64>()
						.ok_or_message(format!("Invalid number: {}", v))?,
				),
				None => bucket.quotas.max_objects,
			},
		};

		self.api_request(UpdateBucketRequest {
			id: bucket.id.clone(),
			body: UpdateBucketRequestBody {
				website_access: None,
				quotas: Some(new_quotas),
			},
		})
		.await?;

		println!("Quotas updated for bucket {:.16}", bucket.id);

		Ok(())
	}
}
