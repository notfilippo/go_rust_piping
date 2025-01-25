run:
  cargo build
  RUST_BACKTRACE=1 go test ./client -count 1 -v
