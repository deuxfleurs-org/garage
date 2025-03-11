use std::sync::Arc;

use chrono::{DateTime, Utc};

use garage_table::*;
use garage_util::time::now_msec;

use garage_model::admin_token_table::*;
use garage_model::garage::Garage;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for ListAdminTokensRequest {
	type Response = ListAdminTokensResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<ListAdminTokensResponse, Error> {
		let now = now_msec();

		let mut res = garage
			.admin_token_table
			.get_range(
				&EmptyKey,
				None,
				Some(KeyFilter::Deleted(DeletedFilter::NotDeleted)),
				10000,
				EnumerationOrder::Forward,
			)
			.await?
			.iter()
			.map(|t| admin_token_info_results(t, now))
			.collect::<Vec<_>>();

		if garage.config.admin.admin_token.is_some() {
			res.insert(
				0,
				GetAdminTokenInfoResponse {
					id: None,
					name: "admin_token (from daemon configuration)".into(),
					expiration: None,
					expired: false,
					scope: vec!["*".into()],
				},
			);
		}

		if garage.config.admin.metrics_token.is_some() {
			res.insert(
				1,
				GetAdminTokenInfoResponse {
					id: None,
					name: "metrics_token (from daemon configuration)".into(),
					expiration: None,
					expired: false,
					scope: vec!["Metrics".into()],
				},
			);
		}

		Ok(ListAdminTokensResponse(res))
	}
}

impl RequestHandler for GetAdminTokenInfoRequest {
	type Response = GetAdminTokenInfoResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<GetAdminTokenInfoResponse, Error> {
		let token = match (self.id, self.search) {
			(Some(id), None) => get_existing_admin_token(garage, &id).await?,
			(None, Some(search)) => {
				let candidates = garage
					.admin_token_table
					.get_range(
						&EmptyKey,
						None,
						Some(KeyFilter::MatchesAndNotDeleted(search.to_string())),
						10,
						EnumerationOrder::Forward,
					)
					.await?
					.into_iter()
					.collect::<Vec<_>>();
				if candidates.len() != 1 {
					return Err(Error::bad_request(format!(
						"{} matching admin tokens",
						candidates.len()
					)));
				}
				candidates.into_iter().next().unwrap()
			}
			_ => {
				return Err(Error::bad_request(
					"Either id or search must be provided (but not both)",
				));
			}
		};

		Ok(admin_token_info_results(&token, now_msec()))
	}
}

impl RequestHandler for CreateAdminTokenRequest {
	type Response = CreateAdminTokenResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<CreateAdminTokenResponse, Error> {
		let (mut token, secret) = if self.0.name.is_some() {
			AdminApiToken::new("")
		} else {
			AdminApiToken::new(&format!("token_{}", Utc::now().format("%Y%m%d_%H%M")))
		};

		apply_token_updates(&mut token, self.0);

		garage.admin_token_table.insert(&token).await?;

		Ok(CreateAdminTokenResponse {
			secret_token: secret,
			info: admin_token_info_results(&token, now_msec()),
		})
	}
}

impl RequestHandler for UpdateAdminTokenRequest {
	type Response = UpdateAdminTokenResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<UpdateAdminTokenResponse, Error> {
		let mut token = get_existing_admin_token(&garage, &self.id).await?;

		apply_token_updates(&mut token, self.body);

		garage.admin_token_table.insert(&token).await?;

		Ok(UpdateAdminTokenResponse(admin_token_info_results(
			&token,
			now_msec(),
		)))
	}
}

impl RequestHandler for DeleteAdminTokenRequest {
	type Response = DeleteAdminTokenResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<DeleteAdminTokenResponse, Error> {
		let token = get_existing_admin_token(&garage, &self.id).await?;

		garage
			.admin_token_table
			.insert(&AdminApiToken::delete(token.prefix))
			.await?;

		Ok(DeleteAdminTokenResponse)
	}
}

// ---- helpers ----

fn admin_token_info_results(token: &AdminApiToken, now: u64) -> GetAdminTokenInfoResponse {
	let params = token.params().unwrap();

	GetAdminTokenInfoResponse {
		id: Some(token.prefix.clone()),
		name: params.name.get().to_string(),
		expiration: params.expiration.get().map(|x| {
			DateTime::from_timestamp_millis(x as i64).expect("invalid timestamp stored in db")
		}),
		expired: params
			.expiration
			.get()
			.map(|exp| now > exp)
			.unwrap_or(false),
		scope: params.scope.get().0.clone(),
	}
}

async fn get_existing_admin_token(garage: &Garage, id: &String) -> Result<AdminApiToken, Error> {
	garage
		.admin_token_table
		.get(&EmptyKey, id)
		.await?
		.filter(|k| !k.state.is_deleted())
		.ok_or_else(|| Error::NoSuchAdminToken(id.to_string()))
}

fn apply_token_updates(token: &mut AdminApiToken, updates: UpdateAdminTokenRequestBody) {
	let params = token.params_mut().unwrap();

	if let Some(name) = updates.name {
		params.name.update(name);
	}
	if let Some(expiration) = updates.expiration {
		params
			.expiration
			.update(Some(expiration.timestamp_millis() as u64));
	}
	if let Some(scope) = updates.scope {
		params.scope.update(AdminApiTokenScope(scope));
	}
}
