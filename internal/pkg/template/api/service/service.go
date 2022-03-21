package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type Service struct {
	dependencies        dependencies.Container
	lock                *sync.Mutex
	repositoriesManager *repository.Manager
}

const TemplateRepositoriesPullInterval = 5 * time.Minute

func New(d dependencies.Container) (*Service, error) {
	repoManager, err := repository.NewManager(d.Logger())
	if err != nil {
		return nil, err
	}
	s := &Service{
		dependencies:        d,
		lock:                &sync.Mutex{},
		repositoriesManager: repoManager,
	}
	s.StartCron()
	return s, nil
}

func (s *Service) StartCron() {
	go func() {
		interval := TemplateRepositoriesPullInterval

		// Delay start to a rounded time
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C
		s.dependencies.Logger().Info("pull ticker started")

		ticker := time.NewTicker(interval)
		for {
			select {
			case <-s.dependencies.Ctx().Done():
				return
			case <-ticker.C:
				s.pullTemplateRepositories()
			}
		}
	}()
}

func (s *Service) pullTemplateRepositories() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.repositoriesManager.Pull()
}

func (s *Service) IndexRoot(_ dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *Service) HealthCheck(_ dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *Service) IndexEndpoint(_ dependencies.Container) (res *templates.Index, err error) {
	res = &templates.Index{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *Service) Foo(d dependencies.Container, payload *templates.FooPayload) (res string, err error) {
	api, err := d.StorageApi()
	if err != nil {
		return "", err
	}

	token := api.Token()
	msg := fmt.Sprintf("token is OK, owner=%s", token.Owner.Name)

	d.Logger().Info(msg)
	return msg, nil
}
