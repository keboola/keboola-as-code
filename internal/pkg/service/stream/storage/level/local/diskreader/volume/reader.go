// Package volume provides detection and opening of volumes intended for slices reading.
package volume

import (
	"path/filepath"
	"sort"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type readerRef struct {
	diskreader.Reader
}

func (v *Volume) OpenReader(slice *model.Slice) (r diskreader.Reader, err error) {
	// Check context
	if err := v.ctx.Err(); err != nil {
		return nil, errors.PrefixErrorf(err, `reader for slice "%s" cannot be created: volume is closed`, slice.SliceKey.String())
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

	// Check if the reader already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addReader(slice.SliceKey)
	if exists {
		return nil, errors.Errorf(`reader for slice "%s" already exists`, slice.SliceKey.String())
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
		v.removeReader(slice.SliceKey)
	}()

	// Open file
	dirPath := filepath.Join(v.Path(), slice.LocalStorage.Dir)
	filePath := filepath.Join(dirPath, slice.LocalStorage.Filename)
	logger = logger.With(attribute.String("file.path", filePath))
	file, err = v.config.fileOpener(filePath)
	if err == nil {
		logger.Debug(v.ctx, "opened file")
	} else {
		logger.Errorf(v.ctx, `cannot open file "%s": %s`, filePath, err)
		return nil, err
	}

	// Init reader and chain
	r, err = diskreader.New(v.ctx, logger, slice, file, v.readerEvents)
	if err != nil {
		return nil, err
	}

	// Register writer close callback
	r.Events().OnClose(func(r diskreader.Reader, _ error) error {
		v.removeReader(r.SliceKey())
		return nil
	})

	return r, nil
}

func (v *Volume) Readers() (out []diskreader.Reader) {
	v.readersLock.Lock()
	defer v.readersLock.Unlock()

	out = make([]diskreader.Reader, 0, len(v.readers))
	for _, w := range v.readers {
		out = append(out, w)
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
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
