package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const OperationTimeout = 20 * time.Second

type Manager struct {
	ctx          context.Context
	logger       log.Logger
	lock         *sync.Mutex
	repositories map[string]*git.Repository
}

func NewManager(ctx context.Context, logger log.Logger) *Manager {
	return &Manager{
		ctx:          ctx,
		logger:       logger,
		lock:         &sync.Mutex{},
		repositories: make(map[string]*git.Repository),
	}
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
	m.logger.Infof(`checking out repository "%s:%s"`, repositoryDef.Url, repositoryDef.Ref)
	ctx, cancel := context.WithTimeout(m.ctx, OperationTimeout)
	defer cancel()
	repo, err := git.Checkout(ctx, repositoryDef.Url, repositoryDef.Ref, false, m.logger)
	if err != nil {
		return fmt.Errorf(`cannot checkout out repository "%s": %w`, repositoryDef, err)
	}
	m.logger.Infof(`repository checked out "%s"`, repo)
	m.repositories[hash] = repo
	return nil
}

func (m *Manager) Pull() {
	for _, repo := range m.repositories {
		repo := repo
		go func() {
			ctx, cancel := context.WithTimeout(m.ctx, OperationTimeout)
			defer cancel()
			if err := pullRepo(ctx, m.logger, repo); err != nil {
				m.logger.Errorf(`error while updating the repository "%s": %w`, repo, err)
			}
		}()
	}
}

func pullRepo(ctx context.Context, logger log.Logger, repo *git.Repository) error {
	startTime := time.Now()
	logger.Infof(`repository "%s" update started`, repo)
	/* oldHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	} */

	if err := repo.Pull(ctx); err != nil {
		return err
	}

	/* newHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	}

	if oldHash == newHash {
		logger.Infof(`repository "%s" update finished, no change found | %s`, repo, time.Since(startTime))
	} else {
		logger.Infof(`repository "%s" updated from %s to %s | %s`, repo, oldHash, newHash, time.Since(startTime))
	} */
	logger.Infof(`repository "%s" updated | %s`, repo, time.Since(startTime))

	return nil
}
