package volume

import (
	"os"
	"path/filepath"
	"sort"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	sliceDirPerm = 0o750
)

type writerRef struct {
	writer.Writer
}

func (v *Volume) NewWriterFor(slice *model.Slice) (out *writer.EventWriter, err error) {
	// Check context
	if err := v.ctx.Err(); err != nil {
		return nil, errors.Errorf(`writer for slice "%s cannot be created, volume is closed: %w`, slice.SliceKey.String(), err)
	}

	// Setup logger
	logger := v.logger.With(
		attribute.String("projectId", slice.ProjectID.String()),
		attribute.String("branchId", slice.BranchID.String()),
		attribute.String("sourceId", slice.SourceID.String()),
		attribute.String("sinkId", slice.SinkID.String()),
		attribute.String("fileId", slice.FileID.String()),
		attribute.String("sliceId", slice.SliceID.String()),
	)

	// Setup events
	events := v.events.Clone()

	// Check if the writer already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addWriter(slice.SliceKey)
	if exists {
		return nil, errors.Errorf(`writer for slice "%s" already exists`, slice.SliceKey.String())
	}

	// Register writer close callback
	events.OnWriterClose(func(_ writer.Writer, _ error) error {
		v.removeWriter(slice.SliceKey)
		return nil
	})

	// Close resources on a creation error
	var file File
	var chain *writechain.Chain
	defer func() {
		// Ok, update reference
		if err == nil {
			ref.Writer = out
			return
		}

		// Close resources
		if file != nil {
			_ = file.Close()
		}
		// Unregister the writer
		v.removeWriter(slice.SliceKey)
	}()

	// Create directory if not exists
	dirPath := filepath.Join(v.Path(), slice.LocalStorage.Dir)
	if err = os.Mkdir(dirPath, sliceDirPerm); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.Errorf(`cannot create slice directory "%s": %w`, dirPath, err)
	}

	// Open file
	filePath := filepath.Join(dirPath, slice.LocalStorage.Filename)
	logger = logger.With(attribute.String("file.path", filePath))
	file, err = v.config.fileOpener(filePath)
	if err == nil {
		logger.Debug(v.ctx, "opened file")
	} else {
		logger.Errorf(v.ctx, `cannot open file "%s": %s`, filePath, err)
		return nil, err
	}

	// Get file info
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Allocate disk space
	if isNew := stat.Size() == 0; isNew {
		if size := slice.LocalStorage.AllocatedDiskSpace; size != 0 {
			if ok, err := v.config.allocator.Allocate(file, size); ok {
				logger.Debugf(v.ctx, `allocated disk space "%s"`, size)
			} else if err != nil {
				// The error is not fatal
				logger.Errorf(v.ctx, `cannot allocate disk space "%s", allocation skipped: %s`, size, err)
			} else {
				logger.Debug(v.ctx, "disk space allocation is not supported")
			}
		} else {
			logger.Debug(v.ctx, "disk space allocation is disabled")
		}
	}

	// Init writers chain
	chain = writechain.New(logger, file)

	// Create writer via factory
	w, err := v.config.writerFactory(writer.NewBaseWriter(logger, v.clock, slice, dirPath, filePath, chain, events))
	if err != nil {
		return nil, err
	}

	// Wrap the writer to add events dispatching
	return writer.NewEventWriter(w, events)
}

func (v *Volume) Writers() (out []writer.Writer) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()

	out = make([]writer.Writer, 0, len(v.writers))
	for _, w := range v.writers {
		out = append(out, w)
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
}

func (v *Volume) addWriter(k model.SliceKey) (ref *writerRef, exists bool) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()

	key := k.String()
	ref, exists = v.writers[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple writers without being blocked by the lock.
		ref = &writerRef{}
		v.writers[key] = ref
	}

	return ref, exists
}

func (v *Volume) removeWriter(k model.SliceKey) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()
	delete(v.writers, k.String())
}
