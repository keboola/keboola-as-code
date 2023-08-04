.PHONY: build

tools:
	bash ./scripts/tools.sh


generate-model:
	./scripts/generate-model.sh

generate-templates-api:
	SERVICE_NAME=templates bash ./scripts/generate-api.sh

generate-buffer-api:
	SERVICE_NAME=buffer bash ./scripts/generate-api.sh

build:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --rm-dist --snapshot -f ./build/ci/goreleaser.yml

build-local:
	GORELEASER_CURRENT_TAG=0.0.1-dev goreleaser build --single-target --rm-dist --snapshot -f ./build/ci/goreleaser.yml

release:
	goreleaser release --rm-dist -f ./build/ci/goreleaser.yml

release-local:
	goreleaser release --rm-dist --snapshot --skip-publish -f ./build/ci/goreleaser.yml

build-templates-api:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/templates/api)" ./cmd/templates-api

run-templates-api:
	air -c ./provisioning/templates-api/dev/.air-api.toml

build-buffer-api:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/buffer/api)" ./cmd/buffer-api

build-buffer-worker:
	CGO_ENABLED=0 go build -v -mod mod -ldflags "-s -w" -o "$(or $(BUILD_TARGET_PATH), ./target/buffer/worker)" ./cmd/buffer-worker

run-buffer-api:
	air -c ./provisioning/buffer/dev/.air-api.toml

run-buffer-api-once: build-buffer-api
	./target/buffer/api

run-buffer-worker:
	air -c ./provisioning/buffer/dev/.air-worker.toml

run-buffer-worker-once: build-buffer-worker
	./target/buffer/worker

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

tests-buffer-service:
	TEST_PACKAGE=./test/buffer/... bash ./scripts/tests.sh

tests-buffer-service-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/buffer/... bash ./scripts/tests.sh

tests-buffer-api:
	TEST_PACKAGE=./test/buffer/api/... bash ./scripts/tests.sh

tests-buffer-api-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/buffer/api/... bash ./scripts/tests.sh

tests-buffer-worker:
	TEST_PACKAGE=./test/buffer/worker/... bash ./scripts/tests.sh

tests-buffer-worker-verbose:
	TEST_VERBOSE=true TEST_LOG_FORMAT=standard-verbose TEST_PARALLELISM=1 TEST_PARALLELISM_PKG=1 TEST_PACKAGE=./test/buffer/worker/... bash ./scripts/tests.sh

mod:
	bash ./scripts/mod.sh

lint:
	bash ./scripts/lint.sh

fix:
	bash ./scripts/fix.sh

ci: mod lint tests

godoc:
	# Example url: http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all
	godoc -http=0.0.0.0:6060

check-licenses:
	go-licenses check ./... --disallowed_types forbidden,restricted
