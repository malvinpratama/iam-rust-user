IMAGE ?= ghcr.io/malvinpratama/iam-rust-useratest
build:   ; cargo build --release
test:    ; cargo test
clippy:  ; cargo clippy
docker:  ; docker build -t $(IMAGE) .
