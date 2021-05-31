.PHONY: build

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o ./target/bin/local/kbc ./src/main.go

build-cross:
	./cross-compile.sh
