// Package diskwriter provides detection and opening of volumes intended for slices writing.
package diskwriter

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/gofrs/flock"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// DrainFile blocks opening of the volume for writing.
	DrainFile = "drain"
	// LockFile ensures only one opening of the volume for writing.
	LockFile          = "writer.lock"
	volumeIDFileFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	volumeIDFilePerm  = 0o640
)

// Volume represents a local directory intended for slices writing.
type Volume struct {
	id   volume.ID
	spec volume.Spec

	logger       log.Logger
	clock        clockwork.Clock
	writerEvents *events.Events[Writer]
	config       Config
	fileOpener   FileOpener
	allocator    diskalloc.Allocator

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     *sync.WaitGroup

	fsLock *flock.Flock

	drained       *atomic.Bool
	drainFilePath string

	writersLock *sync.Mutex
	writers     map[writerKey]*writerRef
}

type writerRef struct {
	Writer
}

// OpenVolume for writing.
//   - It is checked that the volume path exists.
//   - If the drainFile exists, then writing is prohibited and the function ends with an error.
//   - The IDFile is loaded or generated, it contains storage.ID, unique identifier of the volume.
//   - The lockFile ensures only one opening of the volume for writing.
func OpenVolume(ctx context.Context, logger log.Logger, clock clockwork.Clock, config Config, spec volume.Spec, writerEvents *events.Events[Writer]) (*Volume, error) {
	v := &Volume{
		spec:          spec,
		logger:        logger,
		clock:         clock,
		writerEvents:  writerEvents.Clone(), // clone events passed from volumes collection, so volume specific listeners can be added
		config:        config,
		fileOpener:    DefaultFileOpener{},
		allocator:     diskalloc.DefaultAllocator{},
		wg:            &sync.WaitGroup{},
		drained:       atomic.NewBool(false),
		drainFilePath: filepath.Join(spec.Path, DrainFile),
		writersLock:   &sync.Mutex{},
		writers:       make(map[writerKey]*writerRef),
	}

	if config.OverrideFileOpener != nil {
		v.fileOpener = config.OverrideFileOpener
	}

	if config.Allocation.OverrideAllocator != nil {
		v.allocator = config.Allocation.OverrideAllocator
	}

	v.ctx, v.cancel = context.WithCancelCause(context.WithoutCancel(ctx))

	v.logger = v.logger.WithComponent("volume").With(attribute.String("volume.path", spec.Path))
	v.logger.Infof(ctx, `opening volume`)

	// Read volume ID from the file, create it if not exists.
	// The "local/reader.Volume" is waiting for the file.
	{
		idFilePath := filepath.Join(v.spec.Path, volume.IDFile)
		content, err := os.ReadFile(idFilePath)

		// ID file doesn't exist, create it
		generated := false
		if errors.Is(err, os.ErrNotExist) {
			content = []byte(volume.GenerateID())
			err = createVolumeIDFile(idFilePath, content)
			generated = true
		}

		// Check ID file error
		if err != nil {
			return nil, errors.PrefixErrorf(err, `cannot open volume ID file "%s"`, idFilePath)
		}

		// Store volume ID
		v.id = volume.ID(bytes.TrimSpace(content))
		v.logger = v.logger.With(attribute.String("volume.id", v.id.String()))
		if generated {
			v.logger.Infof(ctx, `generated volume ID`)
		}
	}

	// Create lock file
	{
		v.fsLock = flock.New(filepath.Join(v.spec.Path, LockFile))
		if locked, err := v.fsLock.TryLock(); err != nil {
			return nil, errors.PrefixErrorf(err, `cannot acquire writer lock "%s"`, v.fsLock.Path())
		} else if !locked {
			return nil, errors.Errorf(`cannot acquire writer lock "%s": already locked`, v.fsLock.Path())
		}
	}

	// Check drain file and watch it
	if err := v.watchDrainFile(ctx); err != nil {
		return nil, err
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

func (v *Volume) Events() *events.Events[Writer] {
	return v.writerEvents
}

func (v *Volume) Metadata() volume.Metadata {
	return volume.Metadata{
		ID:   v.id,
		Spec: v.spec,
	}
}

func (v *Volume) Telemetry() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("volume.id", v.ID().String()),
		attribute.String("volume.type", v.Type()),
		attribute.String("volume.label", v.Label()),
		attribute.String("volume.path", v.Path()),
	}
}

func (v *Volume) OpenWriter(sourceNodeID string, sliceKey model.SliceKey, slice localModel.Slice, withBackup bool) (w Writer, err error) {
	// Check context
	if err := v.ctx.Err(); err != nil {
		return nil, errors.PrefixErrorf(err, `disk writer cannot be created: volume "%s" is closed`, sliceKey.VolumeID)
	}

	key := writerKey{
		SliceKey:     sliceKey,
		SourceNodeID: sourceNodeID,
	}

	logger := v.logger.
		With(sliceKey.Telemetry()...).
		With(attribute.String("sourceNode.id", key.SourceNodeID))

	// Check if the writer already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addWriter(key)
	if exists {
		err := errors.Errorf(`disk writer already exists`)
		logger.Error(v.ctx, err.Error())
		return nil, err
	}

	// Close resources on a creation error
	defer func() {
		// Ok, update reference
		if err == nil {
			ref.Writer = w
			return
		}

		// Unregister the writer
		v.removeWriter(key)
	}()

	// Create writer based on config
	w, err = newWriter(v.ctx, logger, v.Path(), v.fileOpener, v.allocator, key, slice, v.writerEvents, withBackup)
	if err != nil {
		return nil, err
	}

	// Register writer close callback
	w.Events().OnClose(func(_ Writer, _ error) error {
		v.removeWriter(key)
		return nil
	})

	return w, nil
}

func (v *Volume) Writers() (out []Writer) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()

	out = make([]Writer, 0, len(v.writers))
	for _, w := range v.writers {
		if w.Writer != nil { // nil == creating a new writer
			out = append(out, w)
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
}

func (v *Volume) Close(ctx context.Context) error {
	errs := errors.NewMultiError()
	v.logger.Info(ctx, "closing volume")

	// Block OpenWriter method, stop FS notifier
	v.cancel(errors.New("diskwriter volume closed"))

	// Close all slice writers
	for _, w := range v.Writers() {
		v.wg.Add(1)
		go func() {
			defer v.wg.Done()
			if err := w.Close(ctx); err != nil {
				errs.Append(errors.PrefixErrorf(err, `cannot close writer for slice "%s"`, w.SliceKey().String()))
			}
		}()
	}

	// Wait for writers closing and FS notifier stopping
	v.wg.Wait()

	// Release the lock
	if err := v.fsLock.Unlock(); err != nil {
		errs.Append(errors.PrefixErrorf(err, `cannot release writer lock "%s"`, v.fsLock.Path()))
	}
	if err := os.Remove(v.fsLock.Path()); err != nil {
		errs.Append(errors.PrefixErrorf(err, `cannot remove writer lock "%s"`, v.fsLock.Path()))
	}

	v.logger.Info(ctx, "closed volume")
	return errs.ErrorOrNil()
}

func (v *Volume) addWriter(key writerKey) (ref *writerRef, exists bool) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()

	ref, exists = v.writers[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple writers without being blocked by the lock.
		ref = &writerRef{}
		v.writers[key] = ref
	}

	return ref, exists
}

func (v *Volume) removeWriter(key writerKey) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()
	delete(v.writers, key)
}

func createVolumeIDFile(path string, content []byte) error {
	f, err := os.OpenFile(path, volumeIDFileFlags, volumeIDFilePerm)
	if err != nil {
		return err
	}

	_, writeErr := f.Write(content)
	syncErr := f.Sync()
	closeErr := f.Close()

	switch {
	case writeErr != nil:
		return writeErr
	case syncErr != nil:
		return syncErr
	case closeErr != nil:
		return closeErr
	default:
		return nil
	}
}
