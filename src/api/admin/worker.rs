use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::Error;
use crate::{Admin, RequestHandler};

#[async_trait]
impl RequestHandler for LocalGetWorkerVariableRequest {
	type Response = LocalGetWorkerVariableResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalGetWorkerVariableResponse, Error> {
		let mut res = HashMap::new();
		if let Some(k) = self.variable {
			res.insert(k.clone(), garage.bg_vars.get(&k)?);
		} else {
			let vars = garage.bg_vars.get_all();
			for (k, v) in vars.iter() {
				res.insert(k.to_string(), v.to_string());
			}
		}
		Ok(LocalGetWorkerVariableResponse(res))
	}
}

#[async_trait]
impl RequestHandler for LocalSetWorkerVariableRequest {
	type Response = LocalSetWorkerVariableResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalSetWorkerVariableResponse, Error> {
		garage.bg_vars.set(&self.variable, &self.value)?;

		Ok(LocalSetWorkerVariableResponse {
			variable: self.variable,
			value: self.value,
		})
	}
}
