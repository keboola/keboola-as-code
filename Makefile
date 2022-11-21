.PHONY: build

tools:
	bash ./scripts/tools.sh

prepare: generate-templates-api generate-buffer-api

generate-templates-api:
	if [ ! -d "internal/pkg/service/templates/api/gen/http" ] || [ -z $(SKIP_API_CODE_REGENERATION) ]; then SERVICE_NAME=templates bash ./scripts/generate-api.sh; fi

generate-buffer-api:
	if [ ! -d "internal/pkg/service/buffer/api/gen/http" ] || [ -z $(SKIP_API_CODE_REGENERATION) ]; then SERVICE_NAME=buffer bash ./scripts/generate-api.sh; fi

build: prepare
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml
build-local: prepare
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release: prepare
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local: prepare
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

TEMPLATES_API_BUILD_TARGET_PATH ?= "./target/templates/api"
build-templates-api: generate-templates-api
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o $(TEMPLATES_API_BUILD_TARGET_PATH) ./cmd/templates-api

run-templates-api: generate-templates-api
	air -c ./provisioning/templates-api/dev/.air-api.toml

BUFFER_API_BUILD_TARGET_PATH ?= "./target/buffer/api"
build-buffer-api: generate-buffer-api
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o $(BUFFER_API_BUILD_TARGET_PATH) ./cmd/buffer-api

BUFFER_WORKER_BUILD_TARGET_PATH ?= "./target/buffer/worker"
build-buffer-worker:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o $(BUFFER_WORKER_BUILD_TARGET_PATH) ./cmd/buffer-worker

run-buffer-api: generate-buffer-api
	air -c ./provisioning/buffer/dev/.air-api.toml

run-buffer-worker:
	air -c ./provisioning/buffer/dev/.air-worker.toml

tests: prepare
	TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-unit: prepare
	TEST_PACKAGE=./internal/pkg/... bash ./scripts/tests.sh

tests-unit-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./internal/pkg... bash ./scripts/tests.sh

tests-cli: prepare
	TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh -run TestCliE2E

tests-cli-verbose: prepare
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh -run TestCliE2E

tests-templates-api: generate-templates-api
	TEST_PACKAGE=./test/api/templates/... bash ./scripts/tests.sh -run TestTemplatesApiE2E

tests-templates-api-verbose: generate-templates-api
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/api/templates/... bash ./scripts/tests.sh -run TestTemplatesApiE2E

tests-buffer-api: generate-buffer-api
	TEST_PACKAGE=./test/api/buffer/... bash ./scripts/tests.sh -run TestBufferApiE2E

tests-buffer-api-verbose: generate-buffer-api
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/api/buffer/... bash ./scripts/tests.sh -run TestBufferApiE2E

mod: prepare
	bash ./scripts/mod.sh

lint: prepare
	bash ./scripts/lint.sh

fix: prepare
	bash ./scripts/fix.sh

ci: mod lint tests

godoc:
	# Example url: http://localhost:6060/pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies/?m=all
	godoc -http=0.0.0.0:6060
