package reader

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// lockFile ensures only one opening of the volume for reading
	lockFile                = "reader.lock"
	waitForVolumeIDInterval = 500 * time.Millisecond
)

type Volume struct {
	config config
	logger log.Logger
	clock  clock.Clock
	path   string

	ctx    context.Context
	cancel context.CancelFunc

	volumeID storage.VolumeID
	lock     *flock.Flock

	readersLock *sync.Mutex
	readers     map[string]*readerRef
}

// OpenVolume volume for writing.
//   - It is checked that the volume path exists.
//   - The local.VolumeIDFile is loaded.
//   - If the local.VolumeIDFile doesn't exist, the function waits until the writer.OpenFile function will create it.
//   - The lockFile ensures only one opening of the volume for reading.
func OpenVolume(ctx context.Context, logger log.Logger, clock clock.Clock, path string, opts ...Option) (*Volume, error) {
	logger.Infof(`opening volume "%s"`, path)
	v := &Volume{
		config:      newConfig(opts),
		logger:      logger,
		clock:       clock,
		path:        path,
		readersLock: &sync.Mutex{},
		readers:     make(map[string]*readerRef),
	}

	v.ctx, v.cancel = context.WithCancel(ctx)

	// Check volume directory
	if err := local.CheckVolumeDir(v.path); err != nil {
		return nil, err
	}

	// Wait for volume ID
	if volumeID, err := v.waitForVolumeID(); err == nil {
		v.volumeID = volumeID
	} else {
		return nil, err
	}

	// Create lock file
	// Note: If it is necessary to use the filesystem mounted in read-only mode,
	// this lock can be removed from the code, if it is ensured that only one reader is running at a time.
	{
		v.lock = flock.New(filepath.Join(v.path, lockFile))
		if locked, err := v.lock.TryLock(); err != nil {
			return nil, errors.Errorf(`cannot acquire reader lock "%s": %w`, v.lock.Path(), err)
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire reader lock "%s": already locked`, v.lock.Path())
		}
	}

	v.logger.Info("opened volume")
	return v, nil
}

func (v *Volume) VolumeID() storage.VolumeID {
	return v.volumeID
}

func (v *Volume) Close() error {
	errs := errors.NewMultiError()
	v.logger.Info("closing volume")

	// Block NewReaderFor method
	v.cancel()

	// Close all slice readers
	wg := &sync.WaitGroup{}
	for _, r := range v.openedReaders() {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Close(); err != nil {
				errs.Append(errors.Errorf(`cannot close reader for slice "%s": %w`, r.SliceKey().String(), err))
			}
		}()
	}
	wg.Wait()

	// Release the lock
	if err := v.lock.Unlock(); err != nil {
		errs.Append(errors.Errorf(`cannot release reader lock "%s": %w`, v.lock.Path(), err))
	}
	if err := os.Remove(v.lock.Path()); err != nil {
		errs.Append(errors.Errorf(`cannot remove reader lock "%s": %w`, v.lock.Path(), err))
	}

	v.logger.Info("closed volume")
	return errs.ErrorOrNil()
}

func (v *Volume) openedReaders() (out []SliceReader) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()
	for _, w := range v.readers {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})
	return out
}

// waitForVolumeID waits for the file with volume ID.
// The file is created by the writer.OpenVolume
// and this reader.OpenVolume is waiting for it.
func (v *Volume) waitForVolumeID() (storage.VolumeID, error) {
	ticker := v.clock.Ticker(waitForVolumeIDInterval)
	defer ticker.Stop()

	timeout := v.config.waitForVolumeIDTimeout
	timeoutC := v.clock.After(timeout)

	path := filepath.Join(v.path, local.VolumeIDFile)
	for {
		// Try open the file
		if content, err := os.ReadFile(path); err == nil {
			return storage.VolumeID(strings.TrimSpace(string(content))), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", errors.Errorf(`cannot open volume ID file "%s": %w`, path, err)
		} else {
			v.logger.Infof(`waiting for volume ID file`)
		}

		select {
		case <-ticker.C:
			// One more attempt
		case <-timeoutC:
			// Stop on timeout
			return "", errors.Errorf(`cannot open volume ID file "%s": waiting timeout after %s`, path, timeout)
		}
	}
}
