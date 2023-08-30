package writer

import (
	"bytes"
	"context"
	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// drainFile blocks opening of the volume for writing
	drainFile = "drain"
	// lockFile ensures only one opening of the volume for writing
	lockFile          = "writer.lock"
	volumeIDFileFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	volumeIDFilePerm  = 0o640
)

// Volume represents a local directory intended for slices writing.
type Volume struct {
	config config
	logger log.Logger
	clock  clock.Clock
	path   string

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	volumeID storage.VolumeID
	lock     *flock.Flock

	writersLock *sync.Mutex
	writers     map[string]*writerRef
}

// OpenVolume volume for writing.
//   - It is checked that the volume path exists.
//   - If the drainFile exists, then writing is prohibited and the function ends with an error.
//   - The local.VolumeIDFile is loaded or generated, it contains storage.VolumeID, unique identifier of the volume.
//   - The lockFile ensures only one opening of the volume for writing.
func OpenVolume(ctx context.Context, logger log.Logger, clock clock.Clock, path string, opts ...Option) (*Volume, error) {
	logger.Infof(`opening volume "%s"`, path)
	v := &Volume{
		config:      newConfig(opts),
		logger:      logger,
		clock:       clock,
		path:        path,
		wg:          &sync.WaitGroup{},
		writersLock: &sync.Mutex{},
		writers:     make(map[string]*writerRef),
	}

	v.ctx, v.cancel = context.WithCancel(ctx)

	// Check volume directory
	if err := local.CheckVolumeDir(v.path); err != nil {
		return nil, err
	}

	// Check if the drain file exists, if so, the volume is blocked for writing
	if _, err := os.Stat(filesystem.Join(v.path, drainFile)); err == nil {
		return nil, errors.Errorf(`cannot open volume for writing: found "%s" file`, drainFile)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// Read volume ID from the file, create it if not exists.
	// The "local/reader.Volume" is waiting for the file.
	{
		idFilePath := filepath.Join(v.path, local.VolumeIDFile)
		content, err := os.ReadFile(idFilePath)

		// VolumeID file doesn't exist, create it
		if errors.Is(err, os.ErrNotExist) {
			id := storage.GenerateVolumeID()
			logger.Infof(`generated volume ID "%s"`, id)
			content = []byte(id)
			err = createVolumeIDFile(idFilePath, content)
		}

		// Check VolumeID file error
		if err != nil {
			return nil, errors.Errorf(`cannot open volume ID file "%s": %w`, idFilePath, err)
		}

		// Store volume ID
		v.volumeID = storage.VolumeID(bytes.TrimSpace(content))
	}

	// Create lock file
	{
		v.lock = flock.New(filepath.Join(v.path, lockFile))
		if locked, err := v.lock.TryLock(); err != nil {
			return nil, errors.Errorf(`cannot acquire writer lock "%s": %w`, v.lock.Path(), err)
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire writer lock "%s": already locked`, v.lock.Path())
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

	// Cancel all operations
	v.cancel()

	// Close all slice writers
	for _, w := range v.openedWriters() {
		if err := w.Close(); err != nil {
			errs.Append(errors.Errorf(`cannot close writer for slice "%s": %w`, w.SliceKey().String(), err))
		}
	}

	// Wait for all operations
	v.wg.Wait()

	// Release the lock
	if err := v.lock.Unlock(); err != nil {
		errs.Append(errors.Errorf(`cannot release writer lock "%s": %w`, v.lock.Path(), err))
	}
	if err := os.Remove(v.lock.Path()); err != nil {
		errs.Append(errors.Errorf(`cannot remove writer lock "%s": %w`, v.lock.Path(), err))
	}

	v.logger.Info("closed volume")
	return errs.ErrorOrNil()
}

func (v *Volume) openedWriters() (out []SliceWriter) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()
	for _, w := range v.writers {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})
	return out
}

func createVolumeIDFile(path string, content []byte) error {
	f, err := os.OpenFile(path, volumeIDFileFlags, volumeIDFilePerm)
	if err != nil {
		return err
	}

	_, writeErr := f.Write(content)
	syncErr := f.Sync()
	closeErr := f.Close()

	if writeErr != nil {
		return writeErr
	} else if syncErr != nil {
		return syncErr
	} else if closeErr != nil {
		return closeErr
	}
	return nil
}
