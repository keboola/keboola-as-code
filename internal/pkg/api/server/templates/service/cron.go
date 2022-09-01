package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
)

const TemplateRepositoriesPullInterval = 5 * time.Minute
const ComponentsUpdateInterval = 5 * time.Minute

func StartPullCron(ctx context.Context, d dependencies.ForServer) error {
	// Get dependencies
	manager := d.RepositoryManager()

	// Start background work
	go func() {
		// Delay start to a rounded time
		interval := TemplateRepositoriesPullInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().Infof("repository pull ticker started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
		ticker := time.NewTicker(interval)
		manager.Pull()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.Pull()
			}
		}
	}()
	return nil
}

func StartComponentsCron(ctx context.Context, d dependencies.ForServer) error {
	// Get dependencies
	provider := d.ComponentsProvider()

	// Start background work
	go func() {
		// Delay start to a rounded time
		interval := ComponentsUpdateInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().Infof("components updater started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
		ticker := time.NewTicker(interval)
		provider.Update(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				provider.Update(ctx)
			}
		}
	}()
	return nil
}
