// Package manager coordinates loading and caching of template repositories and templates.
//
// How it works:
//  - Manager is created by New function.
//  - Default repositories are preloaded in New by Manager.Repository method.
//  - Manager.Repository creates a new CachedRepository by newCachedRepository function.
//  - newCachedRepository function preloads all templates by CachedRepository.loadAllTemplates method.
//  - Manager.Repository calls CachedRepository.markInUse method for every request.
//  - After the request is finished, it must call provided UnlockFn.
//  - Manager.Update is called periodically, it calls CachedRepository.update.
//  - If there has been a change in the underlying git repository, then CachedRepository.update will return an updated copy of the repository.
//  - So there EXIST BOTH a new and an old version at the same time.
//  - Older requests will finish with the old repository version, new ones will use the new version.
//  - When all old requests are completed, freeLock is released, so the free method/goroutine is unblocked.
//  - So the repository and underlying FS are cleaned.
package manager

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	checkoutOp "github.com/keboola/keboola-as-code/pkg/lib/operation/repository/checkout"
	loadRepositoryOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/repository/load"
)

// Manager provides CachedRepository and templates for Templates API requests.
// It also contains list of the default repositories.
type Manager struct {
	ctx    context.Context
	deps   dependencies
	logger log.Logger

	// List of default repositories for the stack/server.
	// This list is individually modified in the Service according to the set project features.
	defaultRepositories []model.TemplateRepository

	repositories     map[string]*CachedRepository
	repositoriesInit *singleflight.Group // each template is load only once
	repositoriesLock *sync.RWMutex       // provides atomic access to the repositories field
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	ServerWaitGroup() *sync.WaitGroup
}

func New(ctx context.Context, defaultRepositories []model.TemplateRepository, d dependencies) (*Manager, error) {
	m := &Manager{
		ctx:                 ctx,
		deps:                d,
		logger:              d.Logger(),
		defaultRepositories: defaultRepositories,
		repositories:        make(map[string]*CachedRepository),
		repositoriesInit:    &singleflight.Group{},
		repositoriesLock:    &sync.RWMutex{},
	}

	// Free all repositories on server shutdown
	serverWg := d.ServerWaitGroup()
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		<-ctx.Done()
		m.Free()
	}()

	// Init default repositories in parallel.
	// It preloads all default repositories and templates in them.
	errors := utils.NewMultiError()
	initWg := &sync.WaitGroup{}
	for _, repo := range defaultRepositories {
		repo := repo
		initWg.Add(1)
		go func() {
			defer initWg.Done()
			if _, err := m.repository(ctx, repo); err != nil {
				errors.Append(err)
			}
		}()
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
	m.repositoriesLock.RLock()
	defer m.repositoriesLock.RUnlock()
	for _, r := range m.repositories {
		out = append(out, r.String())
	}
	sort.Strings(out)
	return out
}

func (m *Manager) Repository(ctx context.Context, ref model.TemplateRepository) (*CachedRepository, UnlockFn, error) {
	cachedRepo, err := m.repository(ctx, ref)
	if err != nil {
		return nil, nil, err
	}

	// Prevented cleaning of the repository if it is outdated but still in use by some API request.
	// UnlockFn must be called when the repository is no longer used.
	unlockFn := cachedRepo.markInUse()
	return cachedRepo, unlockFn, nil
}

func (m *Manager) Update(ctx context.Context) <-chan error {
	errorCh := make(chan error, 1)
	errors := utils.NewMultiError()

	// Lock repositories field (during reading it in following for cycle)
	m.repositoriesLock.Lock()

	// Pull repositories in parallel
	wait := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		oldValue := repo
		wait.Add(1)
		go func() {
			defer wait.Done()

			newValue, updated, err := oldValue.update(ctx)
			if err != nil {
				errors.Append(err)
				return
			}

			if updated {
				// Replace value
				m.repositoriesLock.Lock()
				m.repositories[newValue.Hash()] = newValue
				m.repositoriesLock.Unlock()

				// Free previous value
				oldValue.free()
			}
		}()
	}

	// Unlock repositories field for updates
	m.repositoriesLock.Unlock()

	// Write error to channel if any
	go func() {
		wait.Wait()
		errorCh <- errors.ErrorOrNil()
	}()

	return errorCh
}

func (m *Manager) Free() {
	m.repositoriesLock.RLock()
	defer m.repositoriesLock.RUnlock()

	wg := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		repo := repo
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-repo.free()
		}()
	}

	wg.Wait()
	m.logger.Infof("repository manager cleaned up")
}

func (m *Manager) repository(ctx context.Context, ref model.TemplateRepository) (*CachedRepository, error) {
	hash := ref.Hash()

	// Check if is repository already loaded
	m.repositoriesLock.RLock()
	value, found := m.repositories[hash] // nolint: ifshort
	m.repositoriesLock.RUnlock()
	if found {
		return value, nil
	}

	// Load repository, there is used "single flight" library:
	// the function is called only once, but every caller will get the same results.
	ch := m.repositoriesInit.DoChan(hash, func() (interface{}, error) {
		// Load git repository
		var err error
		var gitRepo git.Repository
		if ref.Type == model.RepositoryTypeGit {
			// Remote repository
			startTime := time.Now()
			m.logger.Infof(`checking out repository "%s:%s"`, ref.Url, ref.Ref)

			// Checkout
			gitRepo, err = checkoutOp.Run(ctx, ref, m.deps)
			if err != nil {
				return nil, err
			}

			// Checkout done
			m.logger.Infof(`checked out repository "%s" | %s`, gitRepo, time.Since(startTime))
		} else {
			// Local directory
			fs, err := aferofs.NewLocalFs(m.deps.Logger(), ref.Url, ".")
			if err != nil {
				return nil, err
			}
			gitRepo = git.NewLocalRepository(ref, fs)
			m.logger.Infof(`found local repository "%s"`, gitRepo)
		}

		// Load content of the template repository
		fs, unlockFn := gitRepo.Fs()
		data, err := loadRepositoryOp.Run(ctx, m.deps, ref, loadRepositoryOp.WithFs(fs, unlockFn))
		if err != nil {
			unlockFn()
			return nil, err
		}

		// Initialize cached repository, preload all templates
		r := newCachedRepository(ctx, m.deps, gitRepo, unlockFn, data)

		// Cache value
		m.repositoriesLock.Lock()
		defer m.repositoriesLock.Unlock()
		m.repositories[hash] = r
		return r, nil
	})

	// Check result
	result := <-ch
	if err := result.Err; err != nil {
		return nil, err
	}
	return result.Val.(*CachedRepository), nil
}
