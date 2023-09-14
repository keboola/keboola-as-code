package volume

import (
	"github.com/fsnotify/fsnotify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"os"
)

func (v *Volume) Drained() bool {
	return v.drained.Load()
}

func (v *Volume) watchDrainFile() error {
	// Check presence of the file
	if err := v.checkDrainFile(); err != nil {
		v.logger.Errorf(`cannot check the drain file: %s`, err)
		return err
	}

	// Check if the watcher is enabled
	if !v.config.watchDrainFile {
		return nil
	}

	// Setup watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		// The error is not fatal, skip watching
		v.logger.Errorf(`cannot create FS watcher: %s`, err)
		return nil
	}
	if err := watcher.Add(v.Path()); err != nil {
		// The error is not fatal, skip watching
		v.logger.Errorf(`cannot add path to the FS watcher "%s": %s`, v.Path(), err)
		return nil
	}

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()

		defer func() {
			if err := watcher.Close(); err != nil {
				v.logger.Warnf(`cannot close FS watcher: %s`, err)
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
					if err := v.checkDrainFile(); err != nil {
						v.logger.Errorf(`cannot check the drain file: %s`, err)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				v.logger.Errorf(`FS watcher error: %s`, err)
			}
		}
	}()

	return nil
}

func (v *Volume) checkDrainFile() error {
	if _, err := os.Stat(v.drainFilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	} else if errors.Is(err, os.ErrNotExist) {
		if v.drained.Swap(false) {
			v.logger.Info("set drained=false")
		}
	} else {
		if !v.drained.Swap(true) {
			v.logger.Info("set drained=true")
		}
	}
	return nil
}
