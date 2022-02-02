use std::process;

pub trait CommandExt {
	fn quiet(&mut self) -> &mut Self;

	fn expect_success_status(&mut self, msg: &str) -> process::ExitStatus;
	fn expect_success_output(&mut self, msg: &str) -> process::Output;
}

impl CommandExt for process::Command {
	fn quiet(&mut self) -> &mut Self {
		self.stdout(process::Stdio::null())
			.stderr(process::Stdio::null())
	}

	fn expect_success_status(&mut self, msg: &str) -> process::ExitStatus {
		let status = self.status().expect(msg);
		status.expect_success(msg);
		status
	}
	fn expect_success_output(&mut self, msg: &str) -> process::Output {
		let output = self.output().expect(msg);
		output.expect_success(msg);
		output
	}
}

pub trait OutputExt {
	fn expect_success(&self, msg: &str);
}

impl OutputExt for process::Output {
	fn expect_success(&self, msg: &str) {
		self.status.expect_success(msg)
	}
}

pub trait ExitStatusExt {
	fn expect_success(&self, msg: &str);
}

impl ExitStatusExt for process::ExitStatus {
	fn expect_success(&self, msg: &str) {
		if !self.success() {
			match self.code() {
				Some(code) => panic!(
					"Command exited with code {code}: {msg}",
					code = code,
					msg = msg
				),
				None => panic!("Command exited with signal: {msg}", msg = msg),
			}
		}
	}
}
