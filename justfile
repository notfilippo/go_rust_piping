run:
  cargo build
  RUST_BACKTRACE=1 go run client/main.go