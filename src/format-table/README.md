# `format_table`

Format tables with a stupid API. [Documentation](https://docs.rs/format_table).

Example:

```rust
let mut table = vec!["product\tquantity\tprice".to_string()];
for (p, q, r) in [("tomato", 12, 15), ("potato", 10, 20), ("rice", 5, 12)] {
	table.push(format!("{}\t{}\t{}", p, q, r));
}
format_table::format_table(table);
```
