.PHONY: build

tools:
	bash ./scripts/tools.sh

build:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml
build-local:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release:
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local:
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

tests:
	bash ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose bash ./scripts/tests.sh

tests-cli:
	TEST_VERBOSE=false TEST_LOG_FORMAT=standard-verbose bash ./scripts/tests.sh -run TestCliE2E

tests-cli-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 bash ./scripts/tests.sh -run TestCliE2E

mod:
	bash ./scripts/mod.sh

lint:
	bash ./scripts/lint.sh

fix:
	bash ./scripts/fix.sh

ci: mod lint tests
