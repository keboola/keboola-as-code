// Package dependencies provides dependencies for other parts of the project.
//
// # Operations
//
// The [command design pattern] is used in this project.
//
// The keyword "operation" is used instead of the name "command", to avoid confusion with a CLI command.
//
// Operations (commands) are defined in the "pkg/lib/operation" module,
// but also internal packages work in the similar way.
//
// The operation (command) consists of:
//   - "dependencies" interface.
//   - "Run" function.
//   - Zero or more parameters (or options).
//
// Example operations:
//   - Version check: [src/github.com/keboola/keboola-as-code/pkg/lib/operation/version/check/operation.go]
//   - Status: [src/github.com/keboola/keboola-as-code/pkg/lib/operation/status/operation.go]
//
// Operations are easily composable and testable because:
//   - Parameters/options are static values.
//   - Only necessary dependencies are defined.
//   - Dependencies can be mocked, see [Mocked].
//
// # Dependency Containers
//
// For easier work with dependencies, there are dependency containers for CLI / API and tests (see [Mocked]).
//
// Dependencies containers for services are in separate packages
//   - [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies]
//   - [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies]
//   - [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies]
//
// Example of difference between CLI and API dependencies implementations:
//   - In the CLI the Storage API token is read from ENV or flag.
//   - In the API the Storage API token is read from HTTP header.
//
// [Lazy] helper allows lazy initialization of a dependency on the first use.
//
// # Common Dependencies
//
// In this package contains common parts of dependencies implementation:
//   - [BaseScope] interface provides basic dependencies (see [NewBaseScope]).
//   - [PublicScope] interface provides dependencies available without authentication (see [NewPublicScope]).
//   - [ProjectScope] interface provides dependencies available after authentication (see [NewProjectDeps]).
//   - [RequestInfo] interface provides information about received HTTP request (see [NewRequestInfo]).
//   - [EtcdClientScope] interface provides etcd client and serialization/deserialization (see [NewEtcdClientScope]).
//   - [TaskScope] interface provides dependencies to run exclusive tasks on cluster nodes (see [NewTaskScope]).
//   - [DistributionScope] interface provides dependencies to distribute a work between multiple cluster nodes (see [NewDistributionScope]).
//   - [OrchestratorScope] interface provides dependencies to trigger tasks based on cluster nodes on etcd events (see [NewOrchestratorScope]).
//   - [Mocked] interface provides dependencies mocked for tests (see [NewMocked]).
//
// [command design pattern]: https://refactoring.guru/design-patterns/command
package dependencies

import (
	"context"
	"io"
	"net/http"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	etcdPkg "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	distributionPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	taskPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// BaseScope contains basic dependencies.
type BaseScope interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Clock() clock.Clock
	HTTPClient() client.Client
	Validator() validator.Validator
	Process() *servicectx.Process
	Stdout() io.Writer
	Stderr() io.Writer
}

// PublicScope dependencies are available from the Storage API and other sources without authentication / Storage API token.
type PublicScope interface {
	Components() *model.ComponentsMap
	ComponentsProvider() *model.ComponentsProvider
	KeboolaPublicAPI() *keboola.PublicAPI
	StackFeatures() keboola.FeaturesMap
	StackServices() keboola.ServicesMap
	StorageAPIHost() string
}

// ProjectScope dependencies require authentication - Storage API token.
type ProjectScope interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider
	ProjectID() keboola.ProjectID
	ProjectBackends() []string
	ProjectName() string
	ProjectFeatures() keboola.FeaturesMap
	StorageAPIToken() keboola.Token
	StorageAPITokenID() string
}

// RequestInfo dependencies provides information about received HTTP request.
type RequestInfo interface {
	RequestID() string
	Request() *http.Request
}

// EtcdClientScope dependencies provides etcd client and serialization/deserialization.
type EtcdClientScope interface {
	EtcdClient() *etcdPkg.Client
	EtcdSerde() *serde.Serde
}

// TaskScope dependencies to run exclusive tasks on cluster nodes.
type TaskScope interface {
	TaskNode() *taskPkg.Node
}

// DistributionScope dependencies to distribute a work between multiple cluster nodes.
type DistributionScope interface {
	DistributionNode() *distributionPkg.Node
}

// DistributedLockScope dependencies to acquire distributed locks in the cluster.
type DistributedLockScope interface {
	DistributedLockProvider() *distlock.Provider
}

// Mocked dependencies for tests.
// All HTTP requests to APIs are handled by the MockedHttpTransport by default.
type Mocked interface {
	BaseScope
	PublicScope
	ProjectScope
	RequestInfo
	EtcdClientScope

	MockControl
}

// MockControl allows modification of mocked scopes and access to the insides in a test.
type MockControl interface {
	DebugLogger() log.DebugLogger
	TestTelemetry() telemetry.ForTest
	TestEtcdConfig() etcdclient.Config
	TestEtcdClient() *etcdPkg.Client
	MockedRequest() *http.Request
	MockedHTTPTransport() *httpmock.MockTransport
	MockedProject(fs filesystem.Fs) *projectPkg.Project
	MockedState() *state.State
}
