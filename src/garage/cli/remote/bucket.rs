//use bytesize::ByteSize;
use format_table::format_table;

use chrono::Local;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::remote::*;
use crate::cli::structs::*;

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
			BucketOperation::CleanupIncompleteUploads(query) => {
				self.cmd_cleanup_incomplete_uploads(query).await
			}
			BucketOperation::InspectObject(query) => self.cmd_inspect_object(query).await,
		}
	}

	pub async fn cmd_list_buckets(&self) -> Result<(), Error> {
		let mut buckets = self.api_request(ListBucketsRequest).await?;

		buckets.0.sort_by_key(|x| x.created);

		let mut table = vec!["ID\tCreated\tGlobal aliases\tLocal aliases".to_string()];
		for bucket in buckets.0.iter() {
			table.push(format!(
				"{:.16}\t{}\t{}\t{}",
				bucket.id,
				bucket.created.with_timezone(&Local).date_naive(),
				table_list_abbr(&bucket.global_aliases),
				table_list_abbr(
					bucket
						.local_aliases
						.iter()
						.map(|x| format!("{}:{}", x.access_key_id, x.alias))
				),
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

		print_bucket_info(&bucket);

		Ok(())
	}

	pub async fn cmd_create_bucket(&self, opt: BucketOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(CreateBucketRequest {
				global_alias: Some(opt.name.clone()),
				local_alias: None,
			})
			.await?;

		print_bucket_info(&bucket.0);

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

		let res = if let Some(key_pat) = &opt.local {
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
			.await?
		} else {
			self.api_request(AddBucketAliasRequest {
				bucket_id: bucket.id.clone(),
				alias: BucketAliasEnum::Global {
					global_alias: opt.new_name.clone(),
				},
			})
			.await?
		};

		print_bucket_info(&res.0);

		Ok(())
	}

	pub async fn cmd_unalias_bucket(&self, opt: UnaliasBucketOpt) -> Result<(), Error> {
		let res = if let Some(key_pat) = &opt.local {
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
			.await?
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
			.await?
		};

		print_bucket_info(&res.0);

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

		let res = self
			.api_request(AllowBucketKeyRequest(BucketKeyPermChangeRequest {
				bucket_id: bucket.id.clone(),
				access_key_id: key.access_key_id.clone(),
				permissions: ApiBucketKeyPerm {
					read: opt.read,
					write: opt.write,
					owner: opt.owner,
				},
			}))
			.await?;

		print_bucket_info(&res.0);

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

		let res = self
			.api_request(DenyBucketKeyRequest(BucketKeyPermChangeRequest {
				bucket_id: bucket.id.clone(),
				access_key_id: key.access_key_id.clone(),
				permissions: ApiBucketKeyPerm {
					read: opt.read,
					write: opt.write,
					owner: opt.owner,
				},
			}))
			.await?;

		print_bucket_info(&res.0);

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

		let res = self
			.api_request(UpdateBucketRequest {
				id: bucket.id,
				body: UpdateBucketRequestBody {
					website_access: Some(wa),
					quotas: None,
				},
			})
			.await?;

		print_bucket_info(&res.0);

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

		let res = self
			.api_request(UpdateBucketRequest {
				id: bucket.id.clone(),
				body: UpdateBucketRequestBody {
					website_access: None,
					quotas: Some(new_quotas),
				},
			})
			.await?;

		print_bucket_info(&res.0);

		Ok(())
	}

	pub async fn cmd_cleanup_incomplete_uploads(
		&self,
		opt: CleanupIncompleteUploadsOpt,
	) -> Result<(), Error> {
		let older_than = parse_duration::parse::parse(&opt.older_than)
			.ok_or_message("Invalid duration passed for --older-than parameter")?;

		for b in opt.buckets.iter() {
			let bucket = self
				.api_request(GetBucketInfoRequest {
					id: None,
					global_alias: None,
					search: Some(b.clone()),
				})
				.await?;

			let res = self
				.api_request(CleanupIncompleteUploadsRequest {
					bucket_id: bucket.id.clone(),
					older_than_secs: older_than.as_secs(),
				})
				.await?;

			if res.uploads_deleted > 0 {
				println!("{:.16}: {} uploads deleted", bucket.id, res.uploads_deleted);
			} else {
				println!("{:.16}: no uploads deleted", bucket.id);
			}
		}

		Ok(())
	}

	pub async fn cmd_inspect_object(&self, opt: InspectObjectOpt) -> Result<(), Error> {
		let bucket = self
			.api_request(GetBucketInfoRequest {
				id: None,
				global_alias: None,
				search: Some(opt.bucket),
			})
			.await?;

		let info = self
			.api_request(InspectObjectRequest {
				bucket_id: bucket.id,
				key: opt.key,
			})
			.await?;

		for ver in info.versions {
			println!("==== OBJECT VERSION ====");
			let mut tab = vec![
				format!("Bucket ID:\t{}", info.bucket_id),
				format!("Key:\t{}", info.key),
				format!("Version ID:\t{}", ver.uuid),
				format!("Timestamp:\t{}", ver.timestamp),
			];
			if let Some(size) = ver.size {
				let bs = bytesize::ByteSize::b(size);
				tab.push(format!(
					"Size:\t{} ({})",
					bs.to_string_as(true),
					bs.to_string_as(false)
				));
				tab.push(format!("Size (exact):\t{}", size));
				if !ver.blocks.is_empty() {
					tab.push(format!("Number of blocks:\t{:?}", ver.blocks.len()));
				}
			}
			if let Some(etag) = ver.etag {
				tab.push(format!("Etag:\t{}", etag));
			}
			tab.extend([
				format!("Encrypted:\t{}", ver.encrypted),
				format!("Uploading:\t{}", ver.uploading),
				format!("Aborted:\t{}", ver.aborted),
				format!("Delete marker:\t{}", ver.delete_marker),
				format!("Inline data:\t{}", ver.inline),
			]);
			if !ver.headers.is_empty() {
				tab.push(String::new());
				tab.extend(ver.headers.iter().map(|(k, v)| format!("{}\t{}", k, v)));
			}
			format_table(tab);

			if !ver.blocks.is_empty() {
				let mut tab = vec!["Part#\tOffset\tBlock hash\tSize".to_string()];
				tab.extend(ver.blocks.iter().map(|b| {
					format!(
						"{:4}\t{:9}\t{}\t{:9}",
						b.part_number, b.offset, b.hash, b.size
					)
				}));
				println!();
				format_table(tab);
			}
			println!();
		}

		Ok(())
	}
}

fn print_bucket_info(bucket: &GetBucketInfoResponse) {
	println!("==== BUCKET INFORMATION ====");

	let mut info = vec![
		format!("Bucket:\t{}", bucket.id),
		format!("Created:\t{}", bucket.created.with_timezone(&Local)),
		String::new(),
		{
			let size = bytesize::ByteSize::b(bucket.bytes as u64);
			format!(
				"Size:\t{} ({})",
				size.to_string_as(true),
				size.to_string_as(false)
			)
		},
		format!("Objects:\t{}", bucket.objects),
	];

	if bucket.unfinished_uploads > 0 {
		info.extend([
			format!(
				"Unfinished uploads:\t{} multipart uploads",
				bucket.unfinished_multipart_uploads
			),
			format!("\t{} including regular uploads", bucket.unfinished_uploads),
			{
				let mpu_size =
					bytesize::ByteSize::b(bucket.unfinished_multipart_upload_bytes as u64);
				format!(
					"Size of unfinished multipart uploads:\t{} ({})",
					mpu_size.to_string_as(true),
					mpu_size.to_string_as(false),
				)
			},
		]);
	}

	info.extend([
		String::new(),
		format!("Website access:\t{}", bucket.website_access),
	]);

	if let Some(wc) = &bucket.website_config {
		info.extend([
			format!("  index document:\t{}", wc.index_document),
			format!(
				"  error document:\t{}",
				wc.error_document.as_deref().unwrap_or("(not defined)")
			),
		]);
	}

	if bucket.quotas.max_size.is_some() || bucket.quotas.max_objects.is_some() {
		info.push(String::new());
		info.push("Quotas:\tenabled".into());
		if let Some(ms) = bucket.quotas.max_size {
			let ms = bytesize::ByteSize::b(ms);
			info.push(format!(
				"  maximum size:\t{} ({})",
				ms.to_string_as(true),
				ms.to_string_as(false)
			));
		}
		if let Some(mo) = bucket.quotas.max_objects {
			info.push(format!("  maximum number of objects:\t{}", mo));
		}
	}

	if !bucket.global_aliases.is_empty() {
		info.push(String::new());
		for (i, alias) in bucket.global_aliases.iter().enumerate() {
			if i == 0 && bucket.global_aliases.len() > 1 {
				info.push(format!("Global aliases:\t{}", alias));
			} else if i == 0 {
				info.push(format!("Global alias:\t{}", alias));
			} else {
				info.push(format!("\t{}", alias));
			}
		}
	}

	format_table(info);

	println!("");
	println!("==== KEYS FOR THIS BUCKET ====");
	let mut key_info = vec!["Permissions\tAccess key\t\tLocal aliases".to_string()];
	key_info.extend(bucket.keys.iter().map(|key| {
		let rflag = if key.permissions.read { "R" } else { " " };
		let wflag = if key.permissions.write { "W" } else { " " };
		let oflag = if key.permissions.owner { "O" } else { " " };
		format!(
			"{}{}{}\t{}\t{}\t{}",
			rflag,
			wflag,
			oflag,
			key.access_key_id,
			key.name,
			key.bucket_local_aliases.to_vec().join(","),
		)
	}));
	format_table(key_info);
}
