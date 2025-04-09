.PHONY: build

generate-model:
	./scripts/generate-model.sh

generate-protobuf:
	./scripts/generate-protobuf.sh

generate-appsproxy-api:
	SERVICE_NAME=appsproxy bash ./scripts/generate-api.sh

generate-templates-api:
	SERVICE_NAME=templates bash ./scripts/generate-api.sh

generate-stream-api:
	SERVICE_NAME=stream bash ./scripts/generate-api.sh

build:
	GORELEASER_CURRENT_TAG=0.0.1-dev go tool goreleaser build --clean --snapshot -f ./build/ci/goreleaser.yml

build-local:
	GORELEASER_CURRENT_TAG=0.0.1-dev go tool goreleaser build --single-target --clean --snapshot -f ./build/ci/goreleaser.yml

release:
	go tool goreleaser release --clean -f ./build/ci/goreleaser.yml

release-local:
	go tool goreleaser release --clean --snapshot --skip=publish -f ./build/ci/goreleaser.yml

build-templates-api:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/templates/api)" ./cmd/templates-api

run-templates-api:
	go tool air -c ./provisioning/templates-api/dev/.air-api.toml

build-stream-service:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/stream/service)" ./cmd/stream

build-stream-service-with-race:
	CGO_ENABLED=1 go build -race -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/stream/service)" ./cmd/stream

run-stream-service:
	rm -rf /tmp/stream-volumes && \
    mkdir -p /tmp/stream-volumes/hdd/my-volume && \
	go tool air -c ./provisioning/stream/dev/.air.toml

run-stream-service-once: build-stream-service-with-race
	./target/stream/service api http-source storage-writer storage-reader storage-coordinator

build-apps-proxy:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/apps-proxy/proxy)" ./cmd/apps-proxy

run-apps-proxy:
	go tool air -c ./provisioning/apps-proxy/dev/.air.toml

tests:
	TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PACKAGE=./... bash ./scripts/tests.sh

tests-unit:
	TEST_PACKAGE=./internal/pkg/... bash ./scripts/tests.sh

tests-unit-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./internal/pkg... bash ./scripts/tests.sh

tests-unit-cli:
	TEST_PACKAGE=./internal/pkg/service/cli/... bash ./scripts/tests.sh

tests-unit-templates:
	TEST_PACKAGE=./internal/pkg/service/templates/... bash ./scripts/tests.sh

tests-unit-stream:
	TEST_PACKAGE=./internal/pkg/service/stream/... bash ./scripts/tests.sh

tests-unit-appsproxy:
	TEST_PACKAGE=./internal/pkg/service/appsproxy/... bash ./scripts/tests.sh

tests-unit-common:
	TEST_PACKAGE=./internal/pkg/service/common/... bash ./scripts/tests.sh

# Get all service packages path patterns
SERVICE_PKG_PATTERNS := ./internal/pkg/service/appsproxy/... \
	./internal/pkg/service/cli/... \
	./internal/pkg/service/common/... \
	./internal/pkg/service/stream/... \
	./internal/pkg/service/templates/...

# Test all internal packages *except* those already covered by service-specific targets
tests-unit-core:
	# List all packages, list service packages, filter service ones out, then join with spaces for the command line
	bash -c 'CORE_PKGS=$$(comm -23 <(go list ./internal/pkg/... | sort) <(go list $(SERVICE_PKG_PATTERNS) | sort) | tr "\n" " "); \
	TEST_PACKAGE="$$CORE_PKGS" bash ./scripts/tests.sh'

# Test all service packages in one go
tests-unit-services:
	bash -c 'TEST_PACKAGE="$(SERVICE_PKG_PATTERNS)" bash ./scripts/tests.sh'

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
	go tool godoc -http=0.0.0.0:6060

check-licenses:
	go tool go-licenses check ./... --disallowed_types forbidden,restricted

update:
	go tool go-mod-upgrade
	go mod tidy

install-golangci-lint:
	./scripts/install-golangci-lint.sh
