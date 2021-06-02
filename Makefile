.PHONY: build

build:
	./scripts/compile.sh

build-cross:
	./scripts/cross-compile.sh

tests:
	./scripts/tests.sh

fix:
	./scripts/fix.sh

