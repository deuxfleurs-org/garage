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
		self.expect_success_output(msg).status
	}
	fn expect_success_output(&mut self, msg: &str) -> process::Output {
		let output = self.output().expect(msg);
		if !output.status.success() {
			panic!(
				"{}: command {:?} exited with error {:?}\nSTDOUT: {}\nSTDERR: {}",
				msg,
				self,
				output.status.code(),
				String::from_utf8_lossy(&output.stdout),
				String::from_utf8_lossy(&output.stderr)
			);
		}
		output
	}
}
