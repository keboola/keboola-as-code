.PHONY: build

tools:
	./scripts/tools.sh

build:
	goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml
build-local:
	goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release:
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local:
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

tests:
	TEST_VERBOSE=false ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true ./scripts/tests.sh

tests-functional:
	TEST_VERBOSE=false ./scripts/tests.sh -run TestFunctional

tests-functional-verbose:
	TEST_VERBOSE=true ./scripts/tests.sh -run TestFunctional

lint:
	TEST_VERBOSE=false ./scripts/lint.sh

fix:
	./scripts/fix.sh

ci: lint tests
