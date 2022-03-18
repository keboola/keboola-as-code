package manager

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type Manager struct {
	logger       log.Logger
	repositories map[string]*git.Repository
}

func New(logger log.Logger) (*Manager, error) {
	m := &Manager{
		logger:       logger,
		repositories: make(map[string]*git.Repository),
	}
	if err := m.AddRepository(repository.DefaultRepository()); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) AddRepository(templateRepo model.TemplateRepository) error {
	hash := templateRepo.Hash()
	if _, ok := m.repositories[hash]; ok {
		return fmt.Errorf("repository already exists")
	}

	repo, err := git.CheckoutTemplateRepositoryFull(templateRepo, m.logger)
	if err != nil {
		return err
	}
	m.repositories[hash] = repo
	return nil
}

func (m *Manager) Repositories() []*git.Repository {
	var res []*git.Repository
	for _, repo := range m.repositories {
		res = append(res, repo)
	}
	sort.SliceStable(res, func(i, j int) bool {
		return res[i].Hash() < res[j].Hash()
	})
	return res
}

func (m *Manager) Pull() {
	for _, repo := range m.repositories {
		m.logger.Infof(`repository "%s:%s" is being updated`, repo.Url, repo.Ref)
		err := repo.Pull()
		if err != nil {
			m.logger.Error(err.Error())
			continue
		}
		m.logger.Infof(`repository "%s:%s" update finished`, repo.Url, repo.Ref)
	}
}
