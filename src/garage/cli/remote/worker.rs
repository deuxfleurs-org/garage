use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::remote::*;
use crate::cli::structs::*;

impl Cli {
	pub async fn cmd_worker(&self, cmd: WorkerOperation) -> Result<(), Error> {
		match cmd {
			WorkerOperation::List { opt } => self.cmd_list_workers(opt).await,
			WorkerOperation::Info { tid } => self.cmd_worker_info(tid).await,
			WorkerOperation::Get {
				all_nodes,
				variable,
			} => self.cmd_get_var(all_nodes, variable).await,
			WorkerOperation::Set {
				all_nodes,
				variable,
				value,
			} => self.cmd_set_var(all_nodes, variable, value).await,
		}
	}

	pub async fn cmd_list_workers(&self, opt: WorkerListOpt) -> Result<(), Error> {
		let mut list = self
			.local_api_request(LocalListWorkersRequest {
				busy_only: opt.busy,
				error_only: opt.errors,
			})
			.await?
			.0;

		list.sort_by_key(|info| {
			(
				match info.state {
					WorkerStateResp::Busy | WorkerStateResp::Throttled { .. } => 0,
					WorkerStateResp::Idle => 1,
					WorkerStateResp::Done => 2,
				},
				info.id,
			)
		});

		let mut table =
			vec!["TID\tState\tName\tTranq\tDone\tQueue\tErrors\tConsec\tLast".to_string()];
		let tf = timeago::Formatter::new();
		for info in list.iter() {
			let err_ago = info
				.last_error
				.as_ref()
				.map(|x| tf.convert(Duration::from_secs(x.secs_ago)))
				.unwrap_or_default();
			let (total_err, consec_err) = if info.errors > 0 {
				(info.errors.to_string(), info.consecutive_errors.to_string())
			} else {
				("-".into(), "-".into())
			};

			table.push(format!(
				"{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
				info.id,
				format_worker_state(&info.state),
				info.name,
				info.tranquility
					.as_ref()
					.map(ToString::to_string)
					.unwrap_or_else(|| "-".into()),
				info.progress.as_deref().unwrap_or("-"),
				info.queue_length
					.as_ref()
					.map(ToString::to_string)
					.unwrap_or_else(|| "-".into()),
				total_err,
				consec_err,
				err_ago,
			));
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_worker_info(&self, tid: usize) -> Result<(), Error> {
		let info = self
			.local_api_request(LocalGetWorkerInfoRequest { id: tid as u64 })
			.await?
			.0;

		let mut table = vec![];
		table.push(format!("Task id:\t{}", info.id));
		table.push(format!("Worker name:\t{}", info.name));
		match &info.state {
			WorkerStateResp::Throttled { duration_secs } => {
				table.push(format!(
					"Worker state:\tBusy (throttled, paused for {:.3}s)",
					duration_secs
				));
			}
			s => {
				table.push(format!("Worker state:\t{}", format_worker_state(s)));
			}
		};
		if let Some(tql) = info.tranquility {
			table.push(format!("Tranquility:\t{}", tql));
		}

		table.push("".into());
		table.push(format!("Total errors:\t{}", info.errors));
		table.push(format!("Consecutive errs:\t{}", info.consecutive_errors));
		if let Some(err) = info.last_error {
			table.push(format!("Last error:\t{}", err.message));
			let tf = timeago::Formatter::new();
			table.push(format!(
				"Last error time:\t{}",
				tf.convert(Duration::from_secs(err.secs_ago))
			));
		}

		table.push("".into());
		if let Some(p) = info.progress {
			table.push(format!("Progress:\t{}", p));
		}
		if let Some(ql) = info.queue_length {
			table.push(format!("Queue length:\t{}", ql));
		}
		if let Some(pe) = info.persistent_errors {
			table.push(format!("Persistent errors:\t{}", pe));
		}

		for (i, s) in info.freeform.iter().enumerate() {
			if i == 0 {
				if table.last() != Some(&"".into()) {
					table.push("".into());
				}
				table.push(format!("Message:\t{}", s));
			} else {
				table.push(format!("\t{}", s));
			}
		}
		format_table(table);

		Ok(())
	}

	pub async fn cmd_get_var(&self, all: bool, var: Option<String>) -> Result<(), Error> {
		let res = self
			.api_request(GetWorkerVariableRequest {
				node: if all {
					"*".to_string()
				} else {
					hex::encode(self.rpc_host)
				},
				body: LocalGetWorkerVariableRequest { variable: var },
			})
			.await?;

		let mut table = vec![];
		for (node, vars) in res.success.iter() {
			for (key, val) in vars.0.iter() {
				table.push(format!("{:.16}\t{}\t{}", node, key, val));
			}
		}
		format_table(table);

		for (node, err) in res.error.iter() {
			eprintln!("{:.16}: error: {}", node, err);
		}

		Ok(())
	}

	pub async fn cmd_set_var(
		&self,
		all: bool,
		variable: String,
		value: String,
	) -> Result<(), Error> {
		let res = self
			.api_request(SetWorkerVariableRequest {
				node: if all {
					"*".to_string()
				} else {
					hex::encode(self.rpc_host)
				},
				body: LocalSetWorkerVariableRequest { variable, value },
			})
			.await?;

		let mut table = vec![];
		for (node, kv) in res.success.iter() {
			table.push(format!("{:.16}\t{}\t{}", node, kv.variable, kv.value));
		}
		format_table(table);

		for (node, err) in res.error.iter() {
			eprintln!("{:.16}: error: {}", node, err);
		}

		Ok(())
	}
}

fn format_worker_state(s: &WorkerStateResp) -> &'static str {
	match s {
		WorkerStateResp::Busy => "Busy",
		WorkerStateResp::Throttled { .. } => "Busy*",
		WorkerStateResp::Idle => "Idle",
		WorkerStateResp::Done => "Done",
	}
}
