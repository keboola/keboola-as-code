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

func (m *Manager) AddRepository(ref model.TemplateRepository) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	hash := ref.Hash()
	if _, ok := m.repositories[hash]; ok {
		return fmt.Errorf("repository already exists")
	}

	repo, err := git.CheckoutTemplateRepositoryFull(ref, m.logger)
	if err != nil {
		return err
	}
	m.repositories[hash] = repo
	return nil
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

func (m *Manager) Pull() {
	for _, repo := range m.repositories {
		m.logger.Infof(`repository "%s:%s" is being updated`, repo.Url, repo.Ref)

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
			m.logger.Infof(`repository "%s:%s" update finished, no change found`, repo.Url, repo.Ref)
			return
		}

		m.logger.Infof(`repository "%s:%s" updated from %s to %s`, repo.Url, repo.Ref, oldHash, newHash)
	}
}
