.PHONY: build

build:
	./scripts/compile.sh

build-cross:
	./scripts/cross-compile.sh

tests:
	TEST_VERBOSE=false ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true ./scripts/tests.sh

tests-functional:
	TEST_VERBOSE=false ./scripts/tests.sh -run TestFunctional

tests-functional-verbose:
	TEST_VERBOSE=true ./scripts/tests.sh -run TestFunctional

fix:
	./scripts/fix.sh

