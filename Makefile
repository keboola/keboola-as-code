.PHONY: build

tools:
	bash ./scripts/tools.sh

prepare: generate-code

generate-code:
	if [ ! -d "internal/pkg/api/server/templates/http" ] || [ -z $(SKIP_API_CODE_REGENERATION) ]; then bash ./scripts/generate-templates-api.sh; fi

build: prepare
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml
build-local: prepare
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release: prepare
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local: prepare
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

TEMPLATES_API_BUILD_TARGET_PATH ?= "./target/templates-api/server"
build-templates-api: prepare
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o $(TEMPLATES_API_BUILD_TARGET_PATH) ./cmd/templates-api

run-templates-api: prepare
	air -c ./provisioning/templates-api/dev/.air-templates-api.toml

tests: prepare
	TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-unit: prepare
	TEST_VERBOSE=false TEST_PACKAGE=./internal/pkg/... bash ./scripts/tests.sh

tests-unit-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./internal/pkg... bash ./scripts/tests.sh

tests-cli: prepare
	TEST_VERBOSE=false TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh -run TestCliE2E

tests-cli-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh -run TestCliE2E

tests-templates-api: prepare
	TEST_VERBOSE=false TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./test/api/templates/... bash ./scripts/tests.sh -run TestApiE2E

tests-templates-api-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/api/templates/... bash ./scripts/tests.sh -run TestApiE2E

mod: prepare
	bash ./scripts/mod.sh

lint: generate-code
	bash ./scripts/lint.sh

fix: generate-code
	bash ./scripts/fix.sh

ci: mod lint tests

godoc:
	# Example url: http://localhost:6060/pkg/github.com/keboola/keboola-as-code/internal/pkg/dependencies/?m=all
	godoc -http=0.0.0.0:6060
