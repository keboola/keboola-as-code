package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const OperationTimeout = 30 * time.Second

type Manager struct {
	ctx                 context.Context
	logger              log.Logger
	lock                *sync.Mutex
	defaultRepositories []model.TemplateRepository
	repositories        map[string]*git.Repository
}

func NewManager(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, defaultRepositories []model.TemplateRepository) (*Manager, error) {
	m := &Manager{
		ctx:                 ctx,
		logger:              logger,
		lock:                &sync.Mutex{},
		defaultRepositories: defaultRepositories,
		repositories:        make(map[string]*git.Repository),
	}

	// Delete all temp directories on server shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		m.Free()
	}()

	// Add default repositories
	for _, repo := range defaultRepositories {
		if repo.Type == model.RepositoryTypeGit {
			if err := m.AddRepository(repo); err != nil {
				return nil, err
			}
		}
	}

	return m, nil
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

func (m *Manager) Repository(ref model.TemplateRepository) (*git.Repository, error) {
	// Get or init repository
	if _, found := m.repositories[ref.Hash()]; !found {
		if err := m.AddRepository(ref); err != nil {
			return nil, err
		}
	}
	return m.repositories[ref.Hash()], nil
}

func (m *Manager) AddRepository(repositoryDef model.TemplateRepository) error {
	if repositoryDef.Type != model.RepositoryTypeGit {
		panic("Cannot checkout dir repository")
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	// Check if already exists
	hash := repositoryDef.Hash()
	if _, ok := m.repositories[hash]; ok {
		// repository already exists
		return nil
	}

	// Check out
	startTime := time.Now()
	m.logger.Infof(`checking out repository "%s:%s"`, repositoryDef.Url, repositoryDef.Ref)
	ctx, cancel := context.WithTimeout(m.ctx, OperationTimeout)
	defer cancel()
	repo, err := git.Checkout(ctx, repositoryDef.Url, repositoryDef.Ref, false, m.logger)
	if err != nil {
		return fmt.Errorf(`cannot checkout out repository "%s": %w`, repositoryDef, err)
	}
	m.logger.Infof(`repository checked out "%s" | %s`, repo, time.Since(startTime))
	m.repositories[hash] = repo
	return nil
}

func (m *Manager) Pull() <-chan error {
	errorCh := make(chan error, 1)
	errors := utils.NewMultiError()

	// Pull repositories in parallel
	wg := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		repo := repo
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(m.ctx, OperationTimeout)
			defer cancel()
			if err := pullRepo(ctx, m.logger, repo); err != nil {
				errors.Append(err)
				m.logger.Errorf(`error while updating repository "%s": %w`, repo, err)
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

func pullRepo(ctx context.Context, logger log.Logger, repo *git.Repository) error {
	startTime := time.Now()
	logger.Infof(`repository "%s" update started`, repo)

	oldHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	}

	if err := repo.Pull(ctx); err != nil {
		return err
	}

	newHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	}

	if oldHash == newHash {
		logger.Infof(`repository "%s" update finished, no change found | %s`, repo, time.Since(startTime))
	} else {
		logger.Infof(`repository "%s" updated from %s to %s | %s`, repo, oldHash, newHash, time.Since(startTime))
	}

	return nil
}
