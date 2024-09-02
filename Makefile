.PHONY: build

tools:
	bash ./scripts/tools.sh


generate-model:
	./scripts/generate-model.sh

generate-templates-api:
	SERVICE_NAME=templates bash ./scripts/generate-api.sh

generate-stream-api:
	SERVICE_NAME=stream bash ./scripts/generate-api.sh

build:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --clean --snapshot -f ./build/ci/goreleaser.yml

build-local:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --single-target --clean --snapshot -f ./build/ci/goreleaser.yml

release:
	goreleaser release --clean -f ./build/ci/goreleaser.yml

release-local:
	goreleaser release --clean --snapshot --skip=publish -f ./build/ci/goreleaser.yml

build-templates-api:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/templates/api)" ./cmd/templates-api

run-templates-api:
	air -c ./provisioning/templates-api/dev/.air-api.toml

build-stream-service:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/stream/service)" ./cmd/stream

run-stream-service:
	rm -rf /tmp/stream-volumes && \
    mkdir -p /tmp/stream-volumes/hdd/my-volume && \
	air -c ./provisioning/stream/dev/.air.toml

run-stream-service-once: build-stream-service
	./target/stream/service api http-source storage-writer storage-reader storage-coordinator

build-apps-proxy:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/apps-proxy/proxy)" ./cmd/apps-proxy

run-apps-proxy:
	air -c ./provisioning/apps-proxy/dev/.air.toml

tests:
	TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-unit:
	TEST_PACKAGE=./internal/pkg/... bash ./scripts/tests.sh

tests-unit-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./internal/pkg... bash ./scripts/tests.sh

tests-cli:
	TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh

tests-cli-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/cli/... bash ./scripts/tests.sh

tests-templates-api:
	TEST_PACKAGE=./test/templates/api/... bash ./scripts/tests.sh

tests-templates-api-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/api/templates/... bash ./scripts/tests.sh

tests-stream-service:
	TEST_PACKAGE=./test/stream/... bash ./scripts/tests.sh

tests-stream-service-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/stream/... bash ./scripts/tests.sh

tests-stream-api:
	TEST_PACKAGE=./test/stream/api/... bash ./scripts/tests.sh

tests-stream-api-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/stream/api/... bash ./scripts/tests.sh

tests-stream-worker:
	TEST_PACKAGE=./test/stream/worker/... bash ./scripts/tests.sh

tests-stream-worker-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/stream/worker/... bash ./scripts/tests.sh

mod:
	bash ./scripts/mod.sh

lint:
	bash ./scripts/lint.sh

lint-all:
	bash ./scripts/lint.sh --max-issues-per-linter=0 --max-same-issues=0

fix:
	bash ./scripts/fix.sh

ci: mod lint tests

godoc:
	# Example url: http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all
	godoc -http=0.0.0.0:6060

check-licenses:
	go-licenses check ./... --disallowed_types forbidden,restricted
