package diskwriter

import (
	"context"
	"os"

	"github.com/fsnotify/fsnotify"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (v *Volume) Drained() bool {
	return v.drained.Load()
}

func (v *Volume) watchDrainFile(ctx context.Context) error {
	// Check presence of the file
	if err := v.checkDrainFile(ctx); err != nil {
		v.logger.Errorf(ctx, `cannot check the drain file: %s`, err)
		return err
	}

	// Check if the watcher is enabled
	if !v.config.WatchDrainFile {
		return nil
	}

	// Setup watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		// The error is not fatal, skip watching
		v.logger.Errorf(ctx, `cannot create FS watcher: %s`, err)
		return nil
	}
	if err := watcher.Add(v.Path()); err != nil {
		// The error is not fatal, skip watching
		v.logger.Errorf(ctx, `cannot add path to the FS watcher "%s": %s`, v.Path(), err)
		return nil
	}

	v.wg.Go(func() {

		defer func() {
			if err := watcher.Close(); err != nil {
				v.logger.Warnf(ctx, `cannot close FS watcher: %s`, err)
			}
		}()

		for {
			select {
			case <-v.ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Name == v.drainFilePath {
					if err := v.checkDrainFile(ctx); err != nil {
						v.logger.Errorf(ctx, `cannot check the drain file: %s`, err)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				v.logger.Errorf(ctx, `FS watcher error: %s`, err)
			}
		}
	})

	return nil
}

func (v *Volume) checkDrainFile(ctx context.Context) error {
	if _, err := os.Stat(v.drainFilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	} else if errors.Is(err, os.ErrNotExist) {
		if v.drained.Swap(false) {
			v.logger.Info(ctx, "set drained=false")
		}
	} else {
		if !v.drained.Swap(true) {
			v.logger.Info(ctx, "set drained=true")
		}
	}
	return nil
}
