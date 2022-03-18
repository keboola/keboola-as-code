package manager

import (
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type Manager struct {
	logger       log.Logger
	repositories map[string]*GitRepository
}

type GitRepository struct {
	*model.TemplateRepository
	Dir string
	Fs  filesystem.Fs
}

func New(logger log.Logger) (*Manager, error) {
	m := &Manager{
		logger:       logger,
		repositories: make(map[string]*GitRepository),
	}
	if err := m.AddRepository(repository.DefaultRepository()); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) AddRepository(repo model.TemplateRepository) error {
	hash := repo.Hash()
	if _, ok := m.repositories[hash]; ok {
		return fmt.Errorf("repository already exists")
	}

	fs, dir, err := git.CheckoutTemplateRepositoryFull(repo, m.logger)
	if err != nil {
		return err
	}
	m.repositories[hash] = &GitRepository{
		TemplateRepository: &repo,
		Dir:                dir,
		Fs:                 fs,
	}
	return nil
}

type SortedRepositories []*GitRepository

func (s SortedRepositories) Len() int {
	return len(s)
}
func (s SortedRepositories) Less(i, j int) bool {
	return s[i].Hash() < s[j].Hash()
}
func (s SortedRepositories) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (m *Manager) Repositories() []*GitRepository {
	var res []*GitRepository
	for _, repo := range m.repositories {
		res = append(res, repo)
	}
	sort.Sort(SortedRepositories(res))
	return res
}
