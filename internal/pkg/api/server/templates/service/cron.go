package service

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
)

const TemplateRepositoriesPullInterval = 5 * time.Minute

func StartPullCron(d dependencies.Container) error {
	// Get dependencies
	ctx := d.Ctx()
	manager, err := d.RepositoryManager()
	if err != nil {
		return err
	}

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
