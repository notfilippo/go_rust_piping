# Go <> Rust interop for async tasks

The example covers how to efficiently send data from Go to Rust (and viceversa) 
while still ensuring both runtimes don't block on writes / reads.

Specifically, this example aims to ensure bidirectional communication to execute
a query on data originated in Go, passed to Rust for processing and returned to
Go for consumption.