// Package dependencies provides dependencies for Buffer Service.
//
// # Dependency Containers
//
// This package extends common dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//
// Following dependencies containers are implemented:
//   - [ServiceScope] contains common part of dependencies for all scopes.
//   - [APIScope] contains long-lived dependencies that exist during the entire run of an API node.
//   - [PublicRequestScope] contains short-lived dependencies for a public request without authentication.
//   - [ProjectRequestScope] contains short-lived dependencies for a request with authentication.
//   - [TableSinkScope] contains long-lived dependencies for table sink code.
//
// Dependency containers creation:
//   - [ServiceScope] is created during the creation of [APIScope] or [TableSinkScope].
//   - [APIScope] is created at startup in the API main.go.
//   - [PublicRequestScope] is created for each HTTP request by Muxer.Use callback in main.go.
//   - [ProjectRequestScope] is created for each authenticated HTTP request in the service.APIKeyAuth method.
//   - [TableSinkScope] .....
//
// The package also provides mocked dependency implementations for tests:
//   - [NewMockedServiceScope]
//   - [NewMockedAPIScope]
//   - [NewMockedPublicRequestScope]
//   - [NewMockedProjectRequestScope]
//   - [NewMockedTableSinkScope]
//
// Dependencies injection to service endpoints:
//   - Each service endpoint method gets [PublicRequestScope] as a parameter.
//   - Authorized endpoints gets [ProjectRequestScope] instead.
//   - The injection is generated by "internal/pkg/service/common/goaextension/dependencies" package.
//   - See service implementation for details [src/github.com/keboola/keboola-as-code/internal/pkg/service/biffer/api/service/service.go].
package dependencies

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type ctxKey string

const (
	PublicRequestScopeCtxKey  = ctxKey("PublicRequestScope")
	ProjectRequestScopeCtxKey = ctxKey("ProjectRequestScope")
	BranchRequestScopeCtxKey  = ctxKey("BranchRequestScope")
	SourceRequestScopeCtxKey  = ctxKey("SourceRequestScope")
	SinkRequestScopeCtxKey    = ctxKey("SinkRequestScope")
)

type ServiceScope interface {
	dependencies.BaseScope
	dependencies.PublicScope
	dependencies.EtcdClientScope
	dependencies.TaskScope
	DefinitionRepository() *definitionRepo.Repository
}

type APIScope interface {
	ServiceScope
	APIPublicURL() *url.URL
	HTTPSourcePublicURL() *url.URL
}

type PublicRequestScope interface {
	APIScope
	dependencies.RequestInfo
}

type ProjectRequestScope interface {
	PublicRequestScope
	dependencies.ProjectScope
}

type BranchRequestScope interface {
	ProjectRequestScope
	Branch() definition.Branch
	BranchKey() key.BranchKey
}

type SourceRequestScope interface {
	BranchRequestScope
	SourceKey() key.SourceKey
}

type SinkRequestScope interface {
	SourceRequestScope
	SinkKey() key.SinkKey
}

type TableSinkScope interface {
	ServiceScope
	dependencies.DistributionScope
	dependencies.DistributedLockScope
	StatisticsRepository() *statsRepo.Repository
	StatisticsL1Cache() *cache.L1
	StatisticsL2Cache() *cache.L2
	StorageRepository() *storageRepo.Repository
}

type Mocked interface {
	dependencies.Mocked
	TestConfig() config.Config
}
