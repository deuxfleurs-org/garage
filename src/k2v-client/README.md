Example usage:
```sh
# all these values can be provided on the cli instead
export AWS_ACCESS_KEY_ID=GK123456
export AWS_SECRET_ACCESS_KEY=0123..789
export AWS_REGION=garage
export K2V_ENDPOINT=http://172.30.2.1:3903
export K2V_BUCKET=my-bucket

cargo run --features=cli -- read-range my-partition-key --all

cargo run --features=cli -- insert my-partition-key my-sort-key --text "my string1"
cargo run --features=cli -- insert my-partition-key my-sort-key --text "my string2"
cargo run --features=cli -- insert my-partition-key my-sort-key2 --text "my string"

cargo run --features=cli -- read-range my-partition-key --all

causality=$(cargo run --features=cli -- read my-partition-key my-sort-key2 -b | head -n1)
cargo run --features=cli -- delete my-partition-key my-sort-key2 -c $causality

causality=$(cargo run --features=cli -- read my-partition-key my-sort-key -b | head -n1)
cargo run --features=cli -- insert my-partition-key my-sort-key --text "my string3" -c $causality

cargo run --features=cli -- read-range my-partition-key --all
```
