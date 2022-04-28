package repository

import (
	"context"
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

func NewManager(ctx context.Context, logger log.Logger) (*Manager, error) {
	m := &Manager{
		ctx:          ctx,
		logger:       logger,
		lock:         &sync.Mutex{},
		repositories: make(map[string]*git.Repository),
	}
	return m, m.AddRepository(DefaultRepository())
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
	m.lock.Lock()
	defer m.lock.Unlock()

	hash := repositoryDef.Hash()
	if _, ok := m.repositories[hash]; ok {
		// repository already exists
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, err := git.Checkout(ctx, repositoryDef.Url, repositoryDef.Ref, false, m.logger)
	if err != nil {
		return err
	}

	m.repositories[hash] = repo
	return nil
}

func (m *Manager) Pull() {
	for _, repo := range m.repositories {
		repo := repo
		go func() {
			ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
			defer cancel()
			if err := pullRepo(ctx, m.logger, repo); err != nil {
				m.logger.Errorf(`error while updating the repository "%s": %w`, err)
			}
		}()
	}
}

func pullRepo(ctx context.Context, logger log.Logger, repo *git.Repository) error {
	logger.Infof(`repository "%s" is being updated`, repo)
	oldHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	}

	err = repo.Pull(ctx)
	if err != nil {
		return err
	}

	newHash, err := repo.CommitHash(ctx)
	if err != nil {
		return err
	}

	if oldHash == newHash {
		logger.Infof(`repository "%s" update finished, no change found`, repo)
	} else {
		logger.Infof(`repository "%s" updated from %s to %s`, repo, oldHash, newHash)
	}

	return nil
}
