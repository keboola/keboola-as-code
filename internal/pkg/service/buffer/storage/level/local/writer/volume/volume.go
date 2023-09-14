package volume

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// drainFile blocks opening of the volume for writing.
	drainFile = "drain"
	// lockFile ensures only one opening of the volume for writing.
	lockFile          = "writer.lock"
	volumeIDFileFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	volumeIDFilePerm  = 0o640
)

type volumeInfo = volume.Info

// Volume represents a local directory intended for slices writing.
type Volume struct {
	volumeInfo
	id storage.VolumeID

	config config
	logger log.Logger
	clock  clock.Clock

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	fsLock *flock.Flock

	drained       *atomic.Bool
	drainFilePath string

	writersLock *sync.Mutex
	writers     map[string]*writerRef
}

func NewInfo(path, typ, label string) volume.Info {
	return volume.NewInfo(path, typ, label)
}

// Open volume for writing.
//   - It is checked that the volume path exists.
//   - If the drainFile exists, then writing is prohibited and the function ends with an error.
//   - The local.VolumeIDFile is loaded or generated, it contains storage.VolumeID, unique identifier of the volume.
//   - The lockFile ensures only one opening of the volume for writing.
func Open(ctx context.Context, logger log.Logger, clock clock.Clock, info volumeInfo, opts ...Option) (*Volume, error) {
	logger.Infof(`opening volume "%s"`, info.Path())
	v := &Volume{
		volumeInfo:    info,
		config:        newConfig(opts),
		logger:        logger,
		clock:         clock,
		wg:            &sync.WaitGroup{},
		drained:       atomic.NewBool(false),
		drainFilePath: filesystem.Join(info.Path(), drainFile),
		writersLock:   &sync.Mutex{},
		writers:       make(map[string]*writerRef),
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())

	// Check volume directory
	if err := volume.CheckVolumeDir(v.Path()); err != nil {
		return nil, err
	}

	// Read volume ID from the file, create it if not exists.
	// The "local/reader.Volume" is waiting for the file.
	{
		idFilePath := filepath.Join(v.Path(), volume.IDFile)
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
		v.id = storage.VolumeID(bytes.TrimSpace(content))
	}

	// Create lock file
	{
		v.fsLock = flock.New(filepath.Join(v.Path(), lockFile))
		if locked, err := v.fsLock.TryLock(); err != nil {
			return nil, errors.Errorf(`cannot acquire writer lock "%s": %w`, v.fsLock.Path(), err)
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire writer lock "%s": already locked`, v.fsLock.Path())
		}
	}

	// Check drain file and watch it
	if err := v.watchDrainFile(); err != nil {
		return nil, err
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

	// Block NewWriterFor method, stop FS notifier
	v.cancel()

	// Close all slice writers
	for _, w := range v.Writers() {
		w := w
		v.wg.Add(1)
		go func() {
			defer v.wg.Done()
			if err := w.Close(); err != nil {
				errs.Append(errors.Errorf(`cannot close writer for slice "%s": %w`, w.SliceKey().String(), err))
			}
		}()
	}

	// Wait for writers closing and FS notifier stopping
	v.wg.Wait()

	// Release the lock
	if err := v.fsLock.Unlock(); err != nil {
		errs.Append(errors.Errorf(`cannot release writer lock "%s": %w`, v.fsLock.Path(), err))
	}
	if err := os.Remove(v.fsLock.Path()); err != nil {
		errs.Append(errors.Errorf(`cannot remove writer lock "%s": %w`, v.fsLock.Path(), err))
	}

	v.logger.Info("closed volume")
	return errs.ErrorOrNil()
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
