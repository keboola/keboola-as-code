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

type SortedRepositories []*git.Repository

func (s SortedRepositories) Len() int {
	return len(s)
}

func (s SortedRepositories) Less(i, j int) bool {
	return s[i].Hash() < s[j].Hash()
}

func (s SortedRepositories) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (m *Manager) Repositories() []*git.Repository {
	var res []*git.Repository
	for _, repo := range m.repositories {
		res = append(res, repo)
	}
	sort.Sort(SortedRepositories(res))
	return res
}
