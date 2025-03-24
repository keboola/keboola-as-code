package diskreader

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/jonboulle/clockwork"
	"github.com/sasha-s/go-deadlock"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
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
	cancel context.CancelCauseFunc

	config       Config
	logger       log.Logger
	clock        clockwork.Clock
	readerEvents *events.Events[Reader]

	fsLock *flock.Flock

	readersLock *deadlock.Mutex
	readers     map[string]*readerRef
}

type readerRef struct {
	Reader
}

// OpenVolume for reading.
//   - It is checked that the volume path exists.
//   - The IDFile is loaded.
//   - If the IDFile doesn't exist, the function waits until the writer.Open function will create it.
//   - The LockFile ensures only one opening of the volume for reading.
func OpenVolume(ctx context.Context, logger log.Logger, clock clockwork.Clock, config Config, readerEvents *events.Events[Reader], spec volume.Spec) (*Volume, error) {
	v := &Volume{
		spec:         spec,
		config:       config,
		logger:       logger,
		clock:        clock,
		readerEvents: readerEvents.Clone(), // clone events passed from volumes collection, so volume specific listeners can be added
		readersLock:  &deadlock.Mutex{},
		readers:      make(map[string]*readerRef),
	}

	v.ctx, v.cancel = context.WithCancelCause(context.WithoutCancel(ctx))

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

func (v *Volume) Events() *events.Events[Reader] {
	return v.readerEvents
}

func (v *Volume) Metadata() volume.Metadata {
	return volume.Metadata{
		ID:   v.id,
		Spec: v.spec,
	}
}

func (v *Volume) OpenReader(sliceKey model.SliceKey, slice localModel.Slice, encodingCompression, stagingCompression compression.Config) (r Reader, err error) {
	// Check context
	if err := v.ctx.Err(); err != nil {
		return nil, errors.PrefixErrorf(err, `reader for slice "%s" cannot be created: volume is closed`, sliceKey.String())
	}

	// Setup logger
	logger := v.logger.With(sliceKey.Telemetry()...)

	// Check if the reader already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addReader(sliceKey)
	if exists {
		return nil, errors.Errorf(`reader for slice "%s" already exists`, sliceKey.String())
	}

	// Close resources on a creation error
	var file File
	defer func() {
		// Ok, update reference
		if err == nil {
			ref.Reader = r
			return
		}

		// Close resources
		if file != nil {
			_ = file.Close()
		}

		// Unregister the reader
		v.removeReader(sliceKey)
	}()

	// Get file opener
	var opener FileOpener = DefaultFileOpener{}
	if v.config.OverrideFileOpener != nil {
		opener = v.config.OverrideFileOpener
	}

	// <volumePath>/.FilenamePrefix * FilenameExtension
	// Works for both hidden and visible files.
	path := slice.FileGlob(v.Path())
	if encodingCompression.Type != compression.TypeNone {
		path = slice.FileGlobWithBackup(v.Path())
	}

	// Init reader and chain
	r, err = newReader(
		v.ctx,
		logger,
		sliceKey,
		opener,
		path,
		encodingCompression,
		stagingCompression,
		v.readerEvents,
	)
	if err != nil {
		return nil, err
	}

	// Register writer close callback
	r.Events().OnClose(func(r Reader, _ error) error {
		v.removeReader(r.SliceKey())
		return nil
	})

	return r, nil
}

func (v *Volume) Readers() (out []Reader) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()

	out = make([]Reader, 0, len(v.readers))
	for _, r := range v.readers {
		if r.Reader != nil { // nil == creating a new reader
			out = append(out, r)
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
}

func (v *Volume) Close(ctx context.Context) error {
	errs := errors.NewMultiError()
	v.logger.Infof(ctx, "closing volume")

	// Block OpenReader method
	v.cancel(errors.New("diskreader volume closed"))

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
	ctx, cancel := clockwork.WithTimeout(ctx, v.clock, v.config.WaitForVolumeIDTimeout)
	defer cancel()

	ticker := v.clock.NewTicker(WaitForVolumeIDInterval)
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
		case <-ticker.Chan():
			// One more attempt
		case <-ctx.Done():
			// Stop on context cancellation / timeout
			return "", errors.PrefixErrorf(ctx.Err(), `cannot open volume ID file "%s"`, path)
		}
	}
}

func (v *Volume) addReader(k model.SliceKey) (ref *readerRef, exists bool) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()

	key := k.String()
	ref, exists = v.readers[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple writers without being blocked by the lock.
		ref = &readerRef{}
		v.readers[key] = ref
	}

	return ref, exists
}

func (v *Volume) removeReader(k model.SliceKey) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()
	delete(v.readers, k.String())
}
