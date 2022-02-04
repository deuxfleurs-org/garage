pub fn random_id(len: usize) -> String {
	use rand::distributions::Slice;
	use rand::Rng;

	static ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789.";

	let rng = rand::thread_rng();
	rng.sample_iter(Slice::new(ALPHABET).unwrap())
		.map(|b| char::from(*b))
		.filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
		.take(len)
		.collect()
}
