use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use garage_util::config::DataDirEnum;
use garage_util::data::Hash;
use garage_util::error::{Error, OkOrMessage};
use garage_util::migrate::*;

type Idx = u16;

const DRIVE_NPART: usize = 1024;

const HASH_DRIVE_BYTES: (usize, usize) = (2, 3);

const MARKER_FILE_NAME: &str = "garage-marker";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DataLayout {
	pub(crate) data_dirs: Vec<DataDir>,
	markers: HashMap<PathBuf, String>,

	/// Primary storage location (index in data_dirs) for each partition
	/// = the location where the data is supposed to be, blocks are always
	/// written there (copies in other dirs may be deleted if they exist)
	pub(crate) part_prim: Vec<Idx>,
	/// Secondary storage locations for each partition = locations
	/// where data blocks might be, we check from these dirs when reading
	pub(crate) part_sec: Vec<Vec<Idx>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub(crate) struct DataDir {
	pub(crate) path: PathBuf,
	pub(crate) state: DataDirState,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum DataDirState {
	Active { capacity: u64 },
	ReadOnly,
}

impl DataLayout {
	pub(crate) fn initialize(dirs: &DataDirEnum) -> Result<Self, Error> {
		let data_dirs = make_data_dirs(dirs)?;

		// Split partitions proportionnally to capacity for all drives
		// to affect primary storage location
		let total_cap = data_dirs.iter().filter_map(|x| x.capacity()).sum::<u64>();
		assert!(total_cap > 0);

		let mut part_prim = Vec::with_capacity(DRIVE_NPART);
		let mut cum_cap = 0;
		for (i, dd) in data_dirs.iter().enumerate() {
			if let DataDirState::Active { capacity } = dd.state {
				cum_cap += capacity;
				let n_total = (cum_cap * DRIVE_NPART as u64) / total_cap;
				part_prim.resize(n_total as usize, i as Idx);
			}
		}
		assert_eq!(cum_cap, total_cap);
		assert_eq!(part_prim.len(), DRIVE_NPART);

		// If any of the storage locations is non-empty, it probably existed before
		// this algorithm was added, so add it as a secondary storage location for all partitions
		// to make sure existing files are not lost
		let mut part_sec = vec![vec![]; DRIVE_NPART];
		for (i, dd) in data_dirs.iter().enumerate() {
			if dir_not_empty(&dd.path)? {
				for (sec, prim) in part_sec.iter_mut().zip(part_prim.iter()) {
					if *prim != i as Idx {
						sec.push(i as Idx);
					}
				}
			}
		}

		Ok(Self {
			data_dirs,
			markers: HashMap::new(),
			part_prim,
			part_sec,
		})
	}

	pub(crate) fn update(self, dirs: &DataDirEnum) -> Result<Self, Error> {
		// Make list of new data directories, exit if nothing changed
		let data_dirs = make_data_dirs(dirs)?;
		if data_dirs == self.data_dirs {
			return Ok(self);
		}

		let total_cap = data_dirs.iter().filter_map(|x| x.capacity()).sum::<u64>();
		assert!(total_cap > 0);

		// Compute mapping of old indices to new indices
		let old2new = self
			.data_dirs
			.iter()
			.map(|x| {
				data_dirs
					.iter()
					.position(|y| y.path == x.path)
					.map(|x| x as Idx)
			})
			.collect::<Vec<_>>();

		// Compute secondary location list for partitions based on existing
		// folders, translating indices from old to new
		let mut part_sec = self
			.part_sec
			.iter()
			.map(|dl| {
				dl.iter()
					.filter_map(|old| old2new.get(*old as usize).copied().flatten())
					.collect::<Vec<_>>()
			})
			.collect::<Vec<_>>();

		// Compute a vector that, for each data dir,
		// contains the list of partitions primarily stored on that drive
		let mut dir_prim = vec![vec![]; data_dirs.len()];
		for (ipart, prim) in self.part_prim.iter().enumerate() {
			if let Some(new) = old2new.get(*prim as usize).copied().flatten() {
				dir_prim[new as usize].push(ipart);
			}
		}

		// Compute the target number of partitions per data directory
		let mut cum_cap = 0;
		let mut npart_per_dir = vec![0; data_dirs.len()];
		for (idir, dd) in data_dirs.iter().enumerate() {
			if let DataDirState::Active { capacity } = dd.state {
				let begin = (cum_cap * DRIVE_NPART as u64) / total_cap;
				cum_cap += capacity;
				let end = (cum_cap * DRIVE_NPART as u64) / total_cap;
				npart_per_dir[idir] = (end - begin) as usize;
			}
		}
		assert_eq!(cum_cap, total_cap);
		assert_eq!(npart_per_dir.iter().sum::<usize>(), DRIVE_NPART);

		// For all directories that have too many primary partitions,
		// move that partition to secondary
		for (idir, (parts, tgt_npart)) in dir_prim.iter_mut().zip(npart_per_dir.iter()).enumerate()
		{
			while parts.len() > *tgt_npart {
				let part = parts.pop().unwrap();
				if !part_sec[part].contains(&(idir as Idx)) {
					part_sec[part].push(idir as Idx);
				}
			}
		}

		// Calculate the vector of primary partition dir index
		let mut part_prim = vec![None; DRIVE_NPART];
		for (idir, parts) in dir_prim.iter().enumerate() {
			for part in parts.iter() {
				assert!(part_prim[*part].is_none());
				part_prim[*part] = Some(idir as Idx)
			}
		}

		// Calculate a vector of unassigned partitions
		let mut unassigned = part_prim
			.iter()
			.enumerate()
			.filter(|(_, dir)| dir.is_none())
			.map(|(ipart, _)| ipart)
			.collect::<Vec<_>>();

		// For all directories that don't have enough primary partitions,
		// add partitions from unassigned
		for (idir, (parts, tgt_npart)) in dir_prim.iter_mut().zip(npart_per_dir.iter()).enumerate()
		{
			if parts.len() < *tgt_npart {
				let required = *tgt_npart - parts.len();
				assert!(unassigned.len() >= required);
				for _ in 0..required {
					let new_part = unassigned.pop().unwrap();
					part_prim[new_part] = Some(idir as Idx);
					part_sec[new_part].retain(|x| *x != idir as Idx);
				}
			}
		}

		// Sanity checks
		assert!(part_prim.iter().all(|x| x.is_some()));
		assert!(unassigned.is_empty());

		// Transform part_prim from vec of Option<Idx> to vec of Idx
		let part_prim = part_prim
			.into_iter()
			.map(|x| x.unwrap())
			.collect::<Vec<_>>();
		assert!(part_prim.iter().all(|p| data_dirs
			.get(*p as usize)
			.and_then(|x| x.capacity())
			.unwrap_or(0)
			> 0));

		// If any of the newly added storage locations is non-empty,
		// it might have been removed and added again and might contain data,
		// so add it as a secondary storage location for all partitions
		// to make sure existing files are not lost
		for (i, dd) in data_dirs.iter().enumerate() {
			if self.data_dirs.iter().any(|ed| ed.path == dd.path) {
				continue;
			}
			if dir_not_empty(&dd.path)? {
				for (sec, prim) in part_sec.iter_mut().zip(part_prim.iter()) {
					if *prim != i as Idx && !sec.contains(&(i as Idx)) {
						sec.push(i as Idx);
					}
				}
			}
		}

		// Apply newly generated config
		Ok(Self {
			data_dirs,
			markers: self.markers,
			part_prim,
			part_sec,
		})
	}

	pub(crate) fn check_markers(&mut self) -> Result<(), Error> {
		let data_dirs = &self.data_dirs;
		self.markers
			.retain(|k, _| data_dirs.iter().any(|x| x.path == *k));

		for dir in self.data_dirs.iter() {
			let mut marker_path = dir.path.clone();
			marker_path.push(MARKER_FILE_NAME);
			let existing_marker = std::fs::read_to_string(&marker_path).ok();
			match (existing_marker, self.markers.get(&dir.path)) {
				(Some(m1), Some(m2)) => {
					if m1 != *m2 {
						return Err(Error::Message(format!("Mismatched content for marker file `{}` in data directory `{}`. If you moved data directories or changed their mountpoints, you should remove the `data_layout` file in Garage's metadata directory and restart Garage.", MARKER_FILE_NAME, dir.path.display())));
					}
				}
				(None, Some(_)) => {
					return Err(Error::Message(format!("Could not find expected marker file `{}` in data directory `{}`, make sure this data directory is mounted correctly.", MARKER_FILE_NAME, dir.path.display())));
				}
				(Some(mkr), None) => {
					self.markers.insert(dir.path.clone(), mkr);
				}
				(None, None) => {
					let mkr = hex::encode(garage_util::data::gen_uuid().as_slice());
					std::fs::write(&marker_path, &mkr)?;
					self.markers.insert(dir.path.clone(), mkr);
				}
			}
		}

		Ok(())
	}

	pub(crate) fn primary_block_dir(&self, hash: &Hash) -> PathBuf {
		let ipart = self.partition_from(hash);
		let idir = self.part_prim[ipart] as usize;
		self.block_dir_from(hash, &self.data_dirs[idir].path)
	}

	pub(crate) fn secondary_block_dirs<'a>(
		&'a self,
		hash: &'a Hash,
	) -> impl Iterator<Item = PathBuf> + 'a {
		let ipart = self.partition_from(hash);
		self.part_sec[ipart]
			.iter()
			.map(move |idir| self.block_dir_from(hash, &self.data_dirs[*idir as usize].path))
	}

	fn partition_from(&self, hash: &Hash) -> usize {
		u16::from_be_bytes([
			hash.as_slice()[HASH_DRIVE_BYTES.0],
			hash.as_slice()[HASH_DRIVE_BYTES.1],
		]) as usize
			% DRIVE_NPART
	}

	fn block_dir_from(&self, hash: &Hash, dir: &PathBuf) -> PathBuf {
		let mut path = dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
	}

	pub(crate) fn without_secondary_locations(&self) -> Self {
		Self {
			data_dirs: self.data_dirs.clone(),
			markers: self.markers.clone(),
			part_prim: self.part_prim.clone(),
			part_sec: self.part_sec.iter().map(|_| vec![]).collect::<Vec<_>>(),
		}
	}
}

impl InitialFormat for DataLayout {
	const VERSION_MARKER: &'static [u8] = b"G09bmdl";
}

impl DataDir {
	pub fn capacity(&self) -> Option<u64> {
		match self.state {
			DataDirState::Active { capacity } => Some(capacity),
			_ => None,
		}
	}
}

fn make_data_dirs(dirs: &DataDirEnum) -> Result<Vec<DataDir>, Error> {
	let mut data_dirs = vec![];
	match dirs {
		DataDirEnum::Single(path) => data_dirs.push(DataDir {
			path: path.clone(),
			state: DataDirState::Active {
				capacity: 1_000_000_000, // whatever, doesn't matter
			},
		}),
		DataDirEnum::Multiple(dirs) => {
			let mut ok = false;
			for dir in dirs.iter() {
				let state = match &dir.capacity {
					Some(cap) if dir.read_only == false => {
						let capacity = cap.parse::<bytesize::ByteSize>()
							.ok_or_message("invalid capacity value")?.as_u64();
						if capacity == 0 {
							return Err(Error::Message(format!("data directory {} should have non-zero capacity", dir.path.to_string_lossy())));
						}
						ok = true;
						DataDirState::Active {
							capacity,
						}
					}
					None if dir.read_only == true => {
						DataDirState::ReadOnly
					}
					_ => return Err(Error::Message(format!("data directories in data_dir should have a capacity value or be marked read_only, not the case for {}", dir.path.to_string_lossy()))),
				};
				data_dirs.push(DataDir {
					path: dir.path.clone(),
					state,
				});
			}
			if !ok {
				return Err(Error::Message(
					"incorrect data_dir configuration, no primary writable directory specified"
						.into(),
				));
			}
		}
	}
	Ok(data_dirs)
}

fn dir_not_empty(path: &PathBuf) -> Result<bool, Error> {
	for entry in std::fs::read_dir(&path)? {
		let dir = entry?;
		let ft = dir.file_type()?;
		let name = dir.file_name().into_string().ok();
		if ft.is_file() && name.as_deref() == Some(MARKER_FILE_NAME) {
			return Ok(true);
		}
		if ft.is_dir() && name.and_then(|hex| hex::decode(&hex).ok()).is_some() {
			return Ok(true);
		}
	}
	Ok(false)
}
