package reader

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// lockFile ensures only one opening of the volume for reading.
	lockFile                = "reader.lock"
	waitForVolumeIDInterval = 500 * time.Millisecond
)

type volumeInfo = volume.Info

type Volume struct {
	volumeInfo
	id storage.VolumeID

	ctx    context.Context
	cancel context.CancelFunc

	config config
	logger log.Logger
	clock  clock.Clock

	fsLock *flock.Flock

	readersLock *sync.Mutex
	readers     map[string]*readerRef
}

// OpenVolume volume for writing.
//   - It is checked that the volume path exists.
//   - The volume.IDFile is loaded.
//   - If the volume.IDFile doesn't exist, the function waits until the writer.OpenVolume function will create it.
//   - The lockFile ensures only one opening of the volume for reading.
func OpenVolume(ctx context.Context, logger log.Logger, clock clock.Clock, info volumeInfo, opts ...Option) (*Volume, error) {
	logger.Infof(`opening volume "%s"`, info.Path())
	v := &Volume{
		volumeInfo:  info,
		config:      newConfig(opts),
		logger:      logger,
		clock:       clock,
		readersLock: &sync.Mutex{},
		readers:     make(map[string]*readerRef),
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())

	// Check volume directory
	if err := volume.CheckVolumeDir(v.Path()); err != nil {
		return nil, err
	}

	// Wait for volume ID
	if volumeID, err := v.waitForVolumeID(ctx); err == nil {
		v.id = volumeID
	} else {
		return nil, err
	}

	// Create lock file
	// Note: If it is necessary to use the filesystem mounted in read-only mode,
	// this lock can be removed from the code, if it is ensured that only one reader is running at a time.
	{
		v.fsLock = flock.New(filepath.Join(v.Path(), lockFile))
		if locked, err := v.fsLock.TryLock(); err != nil {
			return nil, errors.Errorf(`cannot acquire reader lock "%s": %w`, v.fsLock.Path(), err)
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire reader lock "%s": already locked`, v.fsLock.Path())
		}
	}

	v.logger.Info("opened volume")
	return v, nil
}

func (v *Volume) ID() storage.VolumeID {
	return v.id
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
	if err := v.fsLock.Unlock(); err != nil {
		errs.Append(errors.Errorf(`cannot release reader lock "%s": %w`, v.fsLock.Path(), err))
	}
	if err := os.Remove(v.fsLock.Path()); err != nil {
		errs.Append(errors.Errorf(`cannot remove reader lock "%s": %w`, v.fsLock.Path(), err))
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
func (v *Volume) waitForVolumeID(ctx context.Context) (storage.VolumeID, error) {
	ctx, cancel := v.clock.WithTimeout(ctx, v.config.waitForVolumeIDTimeout)
	defer cancel()

	ticker := v.clock.Ticker(waitForVolumeIDInterval)
	defer ticker.Stop()

	path := filepath.Join(v.Path(), volume.IDFile)
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
		case <-ctx.Done():
			// Stop on context cancellation / timeout
			return "", errors.Errorf(`cannot open volume ID file "%s": %w`, path, ctx.Err())
		}
	}
}
