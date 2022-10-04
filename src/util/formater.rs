pub fn format_table(data: Vec<String>) {
	let data = data
		.iter()
		.map(|s| s.split('\t').collect::<Vec<_>>())
		.collect::<Vec<_>>();

	let columns = data.iter().map(|row| row.len()).fold(0, std::cmp::max);
	let mut column_size = vec![0; columns];

	let mut out = String::new();

	for row in data.iter() {
		for (i, col) in row.iter().enumerate() {
			column_size[i] = std::cmp::max(column_size[i], col.chars().count());
		}
	}

	for row in data.iter() {
		for (col, col_len) in row[..row.len() - 1].iter().zip(column_size.iter()) {
			out.push_str(col);
			(0..col_len - col.chars().count() + 2).for_each(|_| out.push(' '));
		}
		out.push_str(row[row.len() - 1]);
		out.push('\n');
	}

	print!("{}", out);
}
