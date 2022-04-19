package repository

import (
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Manager struct {
	logger       log.Logger
	lock         *sync.Mutex
	repositories map[string]*git.Repository
}

func NewManager(logger log.Logger) (*Manager, error) {
	m := &Manager{
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
		return fmt.Errorf("repository already exists")
	}

	repo, err := git.Checkout(repositoryDef.Url, repositoryDef.Ref, false, m.logger)
	if err != nil {
		return err
	}

	m.repositories[hash] = repo
	return nil
}

func (m *Manager) Pull() {
	for _, repo := range m.repositories {
		m.logger.Infof(`repository "%s" is being updated`, repo)

		oldHash, err := repo.CommitHash()
		if err != nil {
			m.logger.Error(err.Error())
			continue
		}

		err = repo.Pull()
		if err != nil {
			m.logger.Error(err.Error())
			continue
		}

		newHash, err := repo.CommitHash()
		if err != nil {
			m.logger.Error(err.Error())
			continue
		}

		if oldHash == newHash {
			m.logger.Infof(`repository "%s" update finished, no change found`, repo)
			return
		}

		m.logger.Infof(`repository "%s" updated from %s to %s`, repo, oldHash, newHash)
	}
}
