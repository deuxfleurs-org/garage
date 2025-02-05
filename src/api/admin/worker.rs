use std::collections::HashMap;
use std::sync::Arc;

use garage_util::background::*;
use garage_util::time::now_msec;

use garage_model::garage::Garage;

use crate::api::*;
use crate::error::Error;
use crate::{Admin, RequestHandler};

impl RequestHandler for LocalListWorkersRequest {
	type Response = LocalListWorkersResponse;

	async fn handle(
		self,
		_garage: &Arc<Garage>,
		admin: &Admin,
	) -> Result<LocalListWorkersResponse, Error> {
		let workers = admin.background.get_worker_info();
		let info = workers
			.into_iter()
			.filter(|(_, w)| {
				(!self.busy_only
					|| matches!(w.state, WorkerState::Busy | WorkerState::Throttled(_)))
					&& (!self.error_only || w.errors > 0)
			})
			.map(|(id, w)| worker_info_to_api(id as u64, w))
			.collect::<Vec<_>>();
		Ok(LocalListWorkersResponse(info))
	}
}

impl RequestHandler for LocalGetWorkerInfoRequest {
	type Response = LocalGetWorkerInfoResponse;

	async fn handle(
		self,
		_garage: &Arc<Garage>,
		admin: &Admin,
	) -> Result<LocalGetWorkerInfoResponse, Error> {
		let info = admin
			.background
			.get_worker_info()
			.get(&(self.id as usize))
			.ok_or(Error::NoSuchWorker(self.id))?
			.clone();
		Ok(LocalGetWorkerInfoResponse(worker_info_to_api(
			self.id, info,
		)))
	}
}

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

// ---- helper functions ----

fn worker_info_to_api(id: u64, info: WorkerInfo) -> WorkerInfoResp {
	WorkerInfoResp {
		id,
		name: info.name,
		state: match info.state {
			WorkerState::Busy => WorkerStateResp::Busy,
			WorkerState::Throttled(t) => WorkerStateResp::Throttled { duration_secs: t },
			WorkerState::Idle => WorkerStateResp::Idle,
			WorkerState::Done => WorkerStateResp::Done,
		},
		errors: info.errors as u64,
		consecutive_errors: info.consecutive_errors as u64,
		last_error: info.last_error.map(|(message, t)| WorkerLastError {
			message,
			secs_ago: now_msec().saturating_sub(t) / 1000,
		}),

		tranquility: info.status.tranquility,
		progress: info.status.progress,
		queue_length: info.status.queue_length,
		persistent_errors: info.status.persistent_errors,
		freeform: info.status.freeform,
	}
}
