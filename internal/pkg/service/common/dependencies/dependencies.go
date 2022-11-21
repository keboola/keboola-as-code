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
// Dependencies containers for API and CLI are in separate packages
//   - CLI dependencies: [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies]
//   - API dependencies: [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/dependencies]
//
// Example of difference between CLI and API dependencies implementations:
//   - In the CLI the Storage API token is read from ENV or flag.
//   - In the API the Storage API token is read from HTTP header.
//
// [Lazy] helper allows lazy initialization of a dependency on the first use.
//
// # Common Dependencies
//
// In this package is shared/common part of dependencies implementation:
//   - [Base] interface contains basic dependencies (see [NewBaseDeps]).
//   - [Public] interface contains dependencies available without authentication (see [NewPublicDeps]).
//   - [Project] interface contains dependencies available after authentication (see [NewProjectDeps]).
//   - [Mocked] interface provides dependencies mocked for tests (see [NewMockedDeps]).
//
// [command design pattern]: https://refactoring.guru/design-patterns/command
package dependencies

import (
	"context"
	"sync"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

// Base contains basic dependencies.
type Base interface {
	Envs() env.Provider
	Tracer() trace.Tracer
	Logger() log.Logger
	HttpClient() client.Client
}

// Public dependencies are available from the Storage API and other sources without authentication / Storage API token.
type Public interface {
	StorageApiHost() string
	StorageApiPublicClient() client.Sender
	StackFeatures() storageapi.FeaturesMap
	StackServices() storageapi.ServicesMap
	Components() *model.ComponentsMap
	ComponentsProvider() *model.ComponentsProvider
	EncryptionApiClient() client.Sender
}

// Project dependencies require authentication / Storage API token.
type Project interface {
	ProjectID() int
	ProjectName() string
	ProjectFeatures() storageapi.FeaturesMap
	StorageApiTokenID() string
	StorageApiClient() client.Sender
	SchedulerApiClient() client.Sender
	JobsQueueApiClient() client.Sender
	SandboxesApiClient() client.Sender
	EventSender() event.Sender
	ObjectIDGeneratorFactory() func(ctx context.Context) *storageapi.TicketProvider
}

// Mocked dependencies for tests.
// All HTTP requests to APIs are handled by the MockedHttpTransport by default.
// Use SetFromTestProject method to connect dependencies to a testing project, if it is needed to call real APIs.
type Mocked interface {
	Base
	Public
	Project
	ServerWaitGroup() *sync.WaitGroup
	SetFromTestProject(project *testproject.Project)
	EnvsMutable() *env.Map
	Options() *options.Options
	DebugLogger() log.DebugLogger
	MockedState() *state.State
	MockedProject(fs filesystem.Fs) *projectPkg.Project
	MockedHttpTransport() *httpmock.MockTransport
}
