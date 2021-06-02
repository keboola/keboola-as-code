.PHONY: build

build:
	./script/compile.sh

build-cross:
	./script/cross-compile.sh

tests:
	./script/tests.sh

fix:
	./script/fix.sh

