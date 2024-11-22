package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManager "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// projectRequestScope implements ProjectRequestScope interface.
type projectRequestScope struct {
	PublicRequestScope
	dependencies.ProjectScope
	logger              log.Logger
	repositories        map[string]*repositoryManager.CachedRepository
	projectRepositories dependencies.Lazy[*model.TemplateRepositories]
}

func NewProjectRequestScope(ctx context.Context, pubScp PublicRequestScope, tokenStr string) (v ProjectRequestScope, err error) {
	ctx, span := pubScp.Telemetry().Tracer().Start(ctx, "keboola.go.templates.api.dependencies.NewProjectRequestScope")
	defer span.End(&err)

	prjScp, err := dependencies.NewProjectDeps(ctx, pubScp, tokenStr)
	if err != nil {
		return nil, err
	}

	return newProjectRequestScope(pubScp, prjScp), nil
}

func newProjectRequestScope(pubScp PublicRequestScope, prjScp dependencies.ProjectScope) *projectRequestScope {
	d := &projectRequestScope{}
	d.PublicRequestScope = pubScp
	d.ProjectScope = prjScp
	d.logger = pubScp.Logger()
	return d
}

func (v *projectRequestScope) Logger() log.Logger {
	return v.logger
}

func (v *projectRequestScope) ProjectRepositories() *model.TemplateRepositories {
	return v.projectRepositories.MustInitAndGet(func() *model.TemplateRepositories {
		// Project repositories are default repositories modified by the project features.
		features := v.ProjectFeatures()
		out := model.NewTemplateRepositories()
		for _, repo := range v.RepositoryManager().DefaultRepositories() {
			switch repo.Name {
			case repository.ComponentsTemplateRepositoryName:
				if repo.Ref == repository.DefaultTemplateRepositoryRefMain && features.Has(repository.FeatureComponentsTemplateRepositoryBeta) {
					repo.Ref = repository.DefaultTemplateRepositoryRefBeta
				}
			case repository.DefaultTemplateRepositoryName:
				if repo.Ref == repository.DefaultTemplateRepositoryRefMain {
					if features.Has(repository.FeatureTemplateRepositoryBeta) {
						repo.Ref = repository.DefaultTemplateRepositoryRefBeta
					} else if features.Has(repository.FeatureTemplateRepositoryDev) {
						repo.Ref = repository.DefaultTemplateRepositoryRefDev
					}
				}
			}
			out.Add(repo)
		}
		return out
	})
}

func (v *projectRequestScope) Template(ctx context.Context, reference model.TemplateRef) (tmpl *template.Template, err error) {
	ctx, span := v.Telemetry().Tracer().Start(ctx, "keboola.go.templates.api.dependencies.Template")
	defer span.End(&err)

	// Get repository
	repo, err := v.cachedTemplateRepository(ctx, reference.Repository())
	if err != nil {
		return nil, err
	}

	// Get template
	return repo.Template(ctx, reference)
}

func (v *projectRequestScope) TemplateRepository(ctx context.Context, definition model.TemplateRepository) (tmpl *repository.Repository, err error) {
	ctx, span := v.Telemetry().Tracer().Start(ctx, "keboola.go.templates.api.dependencies.TemplateRepository")
	defer span.End(&err)

	repo, err := v.cachedTemplateRepository(ctx, definition)
	if err != nil {
		return nil, err
	}

	return repo.Unwrap(), nil
}

func (v *projectRequestScope) cachedTemplateRepository(ctx context.Context, definition model.TemplateRepository) (repo *repositoryManager.CachedRepository, err error) {
	if v.repositories == nil {
		v.repositories = make(map[string]*repositoryManager.CachedRepository)
	}

	if _, ok := ctx.Deadline(); !ok {
		panic(errors.New("to prevent the lock from remaining locked, please use a request context with a deadline"))
	}

	if _, found := v.repositories[definition.Hash()]; !found {
		ctx, span := v.Telemetry().Tracer().Start(ctx, "keboola.go.templates.api.dependencies.cachedTemplateRepository")
		defer span.End(&err)

		// Get git repository and lock it,
		// so repository directory won't be deleted during request (if a new version has been pulled).
		repo, unlockFn, err := v.RepositoryManager().Repository(ctx, definition)
		if err != nil {
			return nil, err
		}

		// Unlock repository after the request,
		go func() {
			<-ctx.Done()
			unlockFn()
		}()

		// Cache value for the request
		v.repositories[definition.Hash()] = repo
	}

	return v.repositories[definition.Hash()], nil
}
