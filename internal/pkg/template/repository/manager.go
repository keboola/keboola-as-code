package repository

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	checkoutOp "github.com/keboola/keboola-as-code/pkg/lib/operation/repository/checkout"
	pullOp "github.com/keboola/keboola-as-code/pkg/lib/operation/repository/pull"
)

const PullTimeout = 30 * time.Second

type Manager struct {
	ctx                 context.Context
	deps                dependencies
	logger              log.Logger
	defaultRepositories []model.TemplateRepository
	initGroup           *singleflight.Group
	repositories        map[string]*git.Repository
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	ServerWaitGroup() *sync.WaitGroup
}

func NewManager(ctx context.Context, defaultRepositories []model.TemplateRepository, d dependencies) (*Manager, error) {
	m := &Manager{
		ctx:                 ctx,
		deps:                d,
		logger:              d.Logger(),
		defaultRepositories: defaultRepositories,
		initGroup:           &singleflight.Group{},
		repositories:        make(map[string]*git.Repository),
	}

	// Delete all temp directories on server shutdown
	serverWg := d.ServerWaitGroup()
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		<-ctx.Done()
		m.Free()
	}()

	// Init default repositories in parallel
	errors := utils.NewMultiError()
	initWg := &sync.WaitGroup{}
	for _, repo := range defaultRepositories {
		repo := repo
		if repo.Type == model.RepositoryTypeGit {
			initWg.Add(1)
			go func() {
				defer initWg.Done()
				if _, err := m.Repository(ctx, repo); err != nil {
					errors.Append(err)
				}
			}()
		}
	}
	initWg.Wait()
	return m, errors.ErrorOrNil()
}

// DefaultRepositories returns list of default repositories configured for the API.
func (m *Manager) DefaultRepositories() []model.TemplateRepository {
	return m.defaultRepositories
}

// ManagedRepositories return list of git repositories which are updated when the Pull method is called.
func (m *Manager) ManagedRepositories() (out []string) {
	for _, r := range m.repositories {
		out = append(out, r.String())
	}
	return out
}

func (m *Manager) Repository(ctx context.Context, repoDef model.TemplateRepository) (*git.Repository, error) {
	hash := repoDef.Hash()

	// Get or init repository
	ch := m.initGroup.DoChan(hash, func() (interface{}, error) {
		if _, found := m.repositories[hash]; !found {
			// Log start
			startTime := time.Now()
			m.logger.Infof(`checking out repository "%s:%s"`, repoDef.Url, repoDef.Ref)

			// Checkout
			repo, err := checkoutOp.Run(ctx, repoDef, m.deps)
			if err != nil {
				return nil, err
			}

			// Done
			m.logger.Infof(`checked out repository "%s" | %s`, repo, time.Since(startTime))
			m.repositories[hash] = repo
		}
		return nil, nil
	})

	// Check error during initialization
	if err := (<-ch).Err; err != nil {
		return nil, err
	}

	return m.repositories[repoDef.Hash()], nil
}

func (m *Manager) Pull(ctx context.Context) <-chan error {
	errorCh := make(chan error, 1)
	errors := utils.NewMultiError()

	// Pull repositories in parallel
	wg := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		repo := repo
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Log start
			startTime := time.Now()
			m.logger.Infof(`repository "%s" update started`, repo)

			// Pull
			result, err := pullOp.Run(ctx, repo, m.deps)
			if err != nil {
				errors.Append(err)
				m.logger.Errorf(`error while updating repository "%s": %w`, repo, err)
			}

			// Done
			if result.Changed {
				m.logger.Infof(`repository "%s" updated from %s to %s | %s`, repo, result.OldHash, result.NewHash, time.Since(startTime))
			} else {
				m.logger.Infof(`repository "%s" update finished, no change found | %s`, repo, time.Since(startTime))
			}
		}()
	}

	// Write error to channel if any
	go func() {
		wg.Wait()
		errorCh <- errors.ErrorOrNil()
	}()

	return errorCh
}

func (m *Manager) Free() {
	wg := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		repo := repo
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-repo.Free()
		}()
	}

	wg.Wait()
	m.logger.Infof("repository manager cleaned up")
}
