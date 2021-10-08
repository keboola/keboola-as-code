.PHONY: build

tools:
	bash ./scripts/tools.sh

build:
	goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml
build-local:
	goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release:
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local:
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

tests:
	TEST_VERBOSE=false bash ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose bash ./scripts/tests.sh

tests-functional:
	TEST_VERBOSE=false TEST_LOG_FORMAT=standard-verbose bash ./scripts/tests.sh -run TestFunctional

tests-functional-verbose:
	TEST_VERBOSE=true bash ./scripts/tests.sh -run TestFunctional

mod:
	bash ./scripts/mod.sh

lint:
	bash ./scripts/lint.sh

fix:
	bash ./scripts/fix.sh

ci: mod lint tests
