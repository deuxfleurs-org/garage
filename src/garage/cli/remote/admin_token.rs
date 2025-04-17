use format_table::format_table;

use chrono::Local;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::remote::*;
use crate::cli::structs::*;

impl Cli {
	pub async fn cmd_admin_token(&self, cmd: AdminTokenOperation) -> Result<(), Error> {
		match cmd {
			AdminTokenOperation::List => self.cmd_list_admin_tokens().await,
			AdminTokenOperation::Info { api_token } => self.cmd_admin_token_info(api_token).await,
			AdminTokenOperation::Create(opt) => self.cmd_create_admin_token(opt).await,
			AdminTokenOperation::Rename {
				api_token,
				new_name,
			} => self.cmd_rename_admin_token(api_token, new_name).await,
			AdminTokenOperation::Set(opt) => self.cmd_update_admin_token(opt).await,
			AdminTokenOperation::Delete { api_token, yes } => {
				self.cmd_delete_admin_token(api_token, yes).await
			}
			AdminTokenOperation::DeleteExpired { yes } => {
				self.cmd_delete_expired_admin_tokens(yes).await
			}
		}
	}

	pub async fn cmd_list_admin_tokens(&self) -> Result<(), Error> {
		let mut list = self.api_request(ListAdminTokensRequest).await?;

		list.0.sort_by_key(|x| x.created);

		let mut table = vec!["ID\tCreated\tName\tExpiration\tScope".to_string()];
		for tok in list.0.iter() {
			let scope = if tok.expired {
				String::new()
			} else {
				table_list_abbr(&tok.scope)
			};
			let exp = if tok.expired {
				"expired".to_string()
			} else {
				tok.expiration
					.map(|x| x.with_timezone(&Local).to_string())
					.unwrap_or("never".into())
			};
			table.push(format!(
				"{}\t{}\t{}\t{}\t{}",
				tok.id.as_deref().unwrap_or("-"),
				tok.created
					.map(|x| x.with_timezone(&Local).date_naive().to_string())
					.unwrap_or("-".into()),
				tok.name,
				exp,
				scope,
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_admin_token_info(&self, search: String) -> Result<(), Error> {
		let info = self
			.api_request(GetAdminTokenInfoRequest {
				id: None,
				search: Some(search),
			})
			.await?;

		print_token_info(&info);

		Ok(())
	}

	pub async fn cmd_create_admin_token(&self, opt: AdminTokenCreateOp) -> Result<(), Error> {
		let res = self
			.api_request(CreateAdminTokenRequest(UpdateAdminTokenRequestBody {
				name: opt.name,
				expiration: parse_expires_in(&opt.expires_in)?,
				never_expires: false,
				scope: opt.scope.map(|s| {
					s.split(",")
						.map(|x| x.trim().to_string())
						.collect::<Vec<_>>()
				}),
			}))
			.await?;

		if opt.quiet {
			println!("{}", res.secret_token);
		} else {
			println!("This is your secret bearer token, it will not be shown again by Garage:");
			println!("\n  {}\n", res.secret_token);
			print_token_info(&res.info);
		}

		Ok(())
	}

	pub async fn cmd_rename_admin_token(&self, old: String, new: String) -> Result<(), Error> {
		let token = self
			.api_request(GetAdminTokenInfoRequest {
				id: None,
				search: Some(old),
			})
			.await?;

		let info = self
			.api_request(UpdateAdminTokenRequest {
				id: token.id.unwrap(),
				body: UpdateAdminTokenRequestBody {
					name: Some(new),
					expiration: None,
					never_expires: false,
					scope: None,
				},
			})
			.await?;

		print_token_info(&info.0);

		Ok(())
	}

	pub async fn cmd_update_admin_token(&self, opt: AdminTokenSetOp) -> Result<(), Error> {
		let token = self
			.api_request(GetAdminTokenInfoRequest {
				id: None,
				search: Some(opt.api_token),
			})
			.await?;

		let info = self
			.api_request(UpdateAdminTokenRequest {
				id: token.id.unwrap(),
				body: UpdateAdminTokenRequestBody {
					name: None,
					expiration: parse_expires_in(&opt.expires_in)?,
					never_expires: opt.never_expires,
					scope: opt.scope.map({
						let mut new_scope = token.scope;
						|scope_str| {
							if let Some(add) = scope_str.strip_prefix("+") {
								for a in add.split(",").map(|x| x.trim().to_string()) {
									if !new_scope.contains(&a) {
										new_scope.push(a);
									}
								}
								new_scope
							} else if let Some(sub) = scope_str.strip_prefix("-") {
								for r in sub.split(",").map(|x| x.trim()) {
									new_scope.retain(|x| x != r);
								}
								new_scope
							} else {
								scope_str
									.split(",")
									.map(|x| x.trim().to_string())
									.collect::<Vec<_>>()
							}
						}
					}),
				},
			})
			.await?;

		print_token_info(&info.0);

		Ok(())
	}

	pub async fn cmd_delete_admin_token(&self, token: String, yes: bool) -> Result<(), Error> {
		let token = self
			.api_request(GetAdminTokenInfoRequest {
				id: None,
				search: Some(token),
			})
			.await?;

		let id = token.id.unwrap();

		if !yes {
			return Err(Error::Message(format!(
				"Add the --yes flag to delete API token `{}` ({})",
				token.name, id
			)));
		}

		self.api_request(DeleteAdminTokenRequest { id }).await?;

		println!("Admin API token has been deleted.");

		Ok(())
	}

	pub async fn cmd_delete_expired_admin_tokens(&self, yes: bool) -> Result<(), Error> {
		let mut list = self.api_request(ListAdminTokensRequest).await?.0;

		list.retain(|tok| tok.expired);

		if !yes {
			return Err(Error::Message(format!(
				"This would delete {} admin API tokens, add the --yes flag to proceed.",
				list.len(),
			)));
		}

		for token in list.iter() {
			let id = token.id.clone().unwrap();
			println!("Deleting token `{}` ({})", token.name, id);
			self.api_request(DeleteAdminTokenRequest { id }).await?;
		}

		println!("{} admin API tokens have been deleted.", list.len());

		Ok(())
	}
}

fn print_token_info(token: &GetAdminTokenInfoResponse) {
	println!("==== ADMINISTRATION TOKEN INFORMATION ====");
	let mut table = vec![
		format!("Token ID:\t{}", token.id.as_ref().unwrap()),
		format!("Token name:\t{}", token.name),
		format!("Created:\t{}", token.created.unwrap().with_timezone(&Local)),
		format!(
			"Validity:\t{}",
			token.expired.then_some("EXPIRED").unwrap_or("valid")
		),
		format!(
			"Expiration:\t{}",
			token
				.expiration
				.map(|x| x.with_timezone(&Local).to_string())
				.unwrap_or("never".into())
		),
		String::new(),
	];

	for (i, scope) in token.scope.iter().enumerate() {
		if i == 0 {
			table.push(format!("Scope:\t{}", scope));
		} else {
			table.push(format!("\t{}", scope));
		}
	}

	format_table(table);
}
