use std::time::Duration;

use format_table::format_table;
use garage_util::data::*;
use garage_util::time::*;

use garage_block::manager::BlockResyncErrorInfo;

use garage_model::s3::mpu_table::MultipartUpload;
use garage_model::s3::version_table::*;

pub fn print_block_error_list(el: Vec<BlockResyncErrorInfo>) {
	let now = now_msec();
	let tf = timeago::Formatter::new();
	let mut tf2 = timeago::Formatter::new();
	tf2.ago("");

	let mut table = vec!["Hash\tRC\tErrors\tLast error\tNext try".into()];
	for e in el {
		let next_try = if e.next_try > now {
			tf2.convert(Duration::from_millis(e.next_try - now))
		} else {
			"asap".to_string()
		};
		table.push(format!(
			"{}\t{}\t{}\t{}\tin {}",
			hex::encode(e.hash.as_slice()),
			e.refcount,
			e.error_count,
			tf.convert(Duration::from_millis(now - e.last_try)),
			next_try
		));
	}
	format_table(table);
}

pub fn print_block_info(
	hash: Hash,
	refcount: u64,
	versions: Vec<Result<Version, Uuid>>,
	uploads: Vec<MultipartUpload>,
) {
	println!("Block hash: {}", hex::encode(hash.as_slice()));
	println!("Refcount: {}", refcount);
	println!();

	let mut table = vec!["Version\tBucket\tKey\tMPU\tDeleted".into()];
	let mut nondeleted_count = 0;
	for v in versions.iter() {
		match v {
			Ok(ver) => {
				match &ver.backlink {
					VersionBacklink::Object { bucket_id, key } => {
						table.push(format!(
							"{:?}\t{:?}\t{}\t\t{:?}",
							ver.uuid,
							bucket_id,
							key,
							ver.deleted.get()
						));
					}
					VersionBacklink::MultipartUpload { upload_id } => {
						let upload = uploads.iter().find(|x| x.upload_id == *upload_id);
						table.push(format!(
							"{:?}\t{:?}\t{}\t{:?}\t{:?}",
							ver.uuid,
							upload.map(|u| u.bucket_id).unwrap_or_default(),
							upload.map(|u| u.key.as_str()).unwrap_or_default(),
							upload_id,
							ver.deleted.get()
						));
					}
				}
				if !ver.deleted.get() {
					nondeleted_count += 1;
				}
			}
			Err(vh) => {
				table.push(format!("{:?}\t\t\t\tyes", vh));
			}
		}
	}
	format_table(table);

	if refcount != nondeleted_count {
		println!();
		println!(
			"Warning: refcount does not match number of non-deleted versions, you should try `garage repair block-rc`."
		);
	}
}
