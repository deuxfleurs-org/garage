//! Format tables with a stupid API.
//!
//! Example:
//!
//! ```rust
//! let mut table = vec!["product\tquantity\tprice".to_string()];
//! for (p, q, r) in [("tomato", 12, 15), ("potato", 10, 20), ("rice", 5, 12)] {
//!     table.push(format!("{}\t{}\t{}", p, q, r));
//! }
//! format_table::format_table(table);
//! ```
//!
//! A table to be formatted is a `Vec<String>`, containing one string per line.
//! Table columns in each line are separated by a `\t` character.

/// Format a table and return the result as a string.
pub fn format_table_to_string(data: Vec<String>) -> String {
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

	out
}

/// Format a table and print the result to stdout.
pub fn format_table(data: Vec<String>) {
	print!("{}", format_table_to_string(data));
}
