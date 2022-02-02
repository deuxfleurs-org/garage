macro_rules! assert_bytes_eq {
	($stream:expr, $bytes:expr) => {
		let data = $stream
			.collect()
			.await
			.expect("Error reading data")
			.into_bytes();

		assert_eq!(data.as_ref(), $bytes);
	};
}
