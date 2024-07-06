package volume

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/gofrs/flock"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// LockFile ensures only one opening of the volume for reading.
	LockFile                = "reader.lock"
	WaitForVolumeIDInterval = 500 * time.Millisecond
)

type Volume struct {
	id   volume.ID
	spec volume.Spec

	ctx    context.Context
	cancel context.CancelFunc

	config       config
	logger       log.Logger
	clock        clock.Clock
	readerEvents *events.Events[reader.Reader]

	fsLock *flock.Flock

	readersLock *sync.Mutex
	readers     map[string]*readerRef
}

// Open volume for writing.
//   - It is checked that the volume path exists.
//   - The IDFile is loaded.
//   - If the IDFile doesn't exist, the function waits until the writer.Open function will create it.
//   - The LockFile ensures only one opening of the volume for reading.
func Open(ctx context.Context, logger log.Logger, clock clock.Clock, readerEvents *events.Events[reader.Reader], spec volume.Spec, opts ...Option) (*Volume, error) {
	v := &Volume{
		spec:         spec,
		config:       newConfig(opts),
		logger:       logger,
		clock:        clock,
		readerEvents: readerEvents.Clone(), // clone events passed from volumes collection, so volume specific listeners can be added
		readersLock:  &sync.Mutex{},
		readers:      make(map[string]*readerRef),
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())

	v.logger.With(attribute.String("volume.path", spec.Path)).Infof(ctx, `opening volume`)

	// Wait for volume ID
	if volumeID, err := v.waitForVolumeID(ctx); err == nil {
		v.id = volumeID
		v.logger = v.logger.With(attribute.String("volume.id", v.id.String()))
	} else {
		return nil, err
	}

	// Create lock file
	// Note: If it is necessary to use the filesystem mounted in read-only mode,
	// this lock can be removed from the code, if it is ensured that only one reader is running at a time.
	{
		v.fsLock = flock.New(filepath.Join(v.spec.Path, LockFile))
		if locked, err := v.fsLock.TryLock(); err != nil {
			return nil, errors.PrefixErrorf(err, `cannot acquire reader lock "%s"`, v.fsLock.Path())
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire reader lock "%s": already locked`, v.fsLock.Path())
		}
	}

	// Log volume details on open.
	// Other log messages contain only the "volume.id", see above.
	v.logger.
		With(
			attribute.String("volume.path", spec.Path),
			attribute.String("volume.type", spec.Type),
			attribute.String("volume.label", spec.Label),
		).
		Info(ctx, "opened volume")
	return v, nil
}

func (v *Volume) Path() string {
	return v.spec.Path
}

func (v *Volume) Type() string {
	return v.spec.Type
}

func (v *Volume) Label() string {
	return v.spec.Label
}

func (v *Volume) ID() volume.ID {
	return v.id
}

func (v *Volume) Events() *events.Events[reader.Reader] {
	return v.readerEvents
}

func (v *Volume) Metadata() volume.Metadata {
	return volume.Metadata{
		ID:   v.id,
		Spec: v.spec,
	}
}

func (v *Volume) Close(ctx context.Context) error {
	errs := errors.NewMultiError()
	v.logger.Infof(ctx, "closing volume")

	// Block OpenReader method
	v.cancel()

	// Close all slice readers
	wg := &sync.WaitGroup{}
	for _, r := range v.Readers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Close(ctx); err != nil {
				errs.Append(errors.PrefixErrorf(err, `cannot close reader for slice "%s"`, r.SliceKey().String()))
			}
		}()
	}
	wg.Wait()

	// Release the lock
	if err := v.fsLock.Unlock(); err != nil {
		errs.Append(errors.PrefixErrorf(err, `cannot release reader lock "%s"`, v.fsLock.Path()))
	}
	if err := os.Remove(v.fsLock.Path()); err != nil {
		errs.Append(errors.PrefixErrorf(err, `cannot remove reader lock "%s"`, v.fsLock.Path()))
	}

	v.logger.Infof(ctx, "closed volume")
	return errs.ErrorOrNil()
}

// waitForVolumeID waits for the file with volume ID.
// The file is created by the writer.Open
// and this reader.Open is waiting for it.
func (v *Volume) waitForVolumeID(ctx context.Context) (volume.ID, error) {
	ctx, cancel := v.clock.WithTimeout(ctx, v.config.waitForVolumeIDTimeout)
	defer cancel()

	ticker := v.clock.Ticker(WaitForVolumeIDInterval)
	defer ticker.Stop()

	path := filepath.Join(v.spec.Path, volume.IDFile)
	for {
		// Try open the file
		if content, err := os.ReadFile(path); err == nil {
			return volume.ID(strings.TrimSpace(string(content))), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", errors.PrefixErrorf(err, `cannot open volume ID file "%s"`, path)
		} else {
			v.logger.Infof(ctx, `waiting for volume ID file`)
		}

		select {
		case <-ticker.C:
			// One more attempt
		case <-ctx.Done():
			// Stop on context cancellation / timeout
			return "", errors.PrefixErrorf(ctx.Err(), `cannot open volume ID file "%s"`, path)
		}
	}
}
