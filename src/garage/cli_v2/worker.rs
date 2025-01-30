//use bytesize::ByteSize;
use format_table::format_table;

use garage_util::error::*;

use garage_api_admin::api::*;

use crate::cli::structs::*;
use crate::cli_v2::*;

impl Cli {
	pub async fn cmd_worker(&self, cmd: WorkerOperation) -> Result<(), Error> {
		match cmd {
			WorkerOperation::Get {
				all_nodes,
				variable,
			} => self.cmd_get_var(all_nodes, variable).await,
			WorkerOperation::Set {
				all_nodes,
				variable,
				value,
			} => self.cmd_set_var(all_nodes, variable, value).await,
			wo => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::Worker(wo),
			)
			.await
			.ok_or_message("cli_v1"),
		}
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
