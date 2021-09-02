.PHONY: build

build:
	goreleaser build --rm-dist --snapshot

release:
	goreleaser release --rm-dist
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
