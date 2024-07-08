package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type writerRef struct {
	diskwriter.Writer
}

func (v *Volume) OpenWriter(slice *model.Slice) (w diskwriter.Writer, err error) {
	// Check context
	if err := v.ctx.Err(); err != nil {
		return nil, errors.PrefixErrorf(err, `writer for slice "%s" cannot be created: volume is closed`, slice.SliceKey.String())
	}

	// Check if the writer already exists, if not, register an empty reference to unlock immediately
	ref, exists := v.addWriter(slice.SliceKey)
	if exists {
		return nil, errors.Errorf(`writer for slice "%s" already exists`, slice.SliceKey.String())
	}

	// Close resources on a creation error
	defer func() {
		// Ok, update reference
		if err == nil {
			ref.Writer = w
			return
		}

		// Unregister the writer
		v.removeWriter(slice.SliceKey)
	}()

	// Create writer
	w, err = diskwriter.New(v.ctx, v.logger, v.config, v.Path(), slice, v.writerEvents)
	if err != nil {
		return nil, err
	}

	// Register writer close callback
	w.Events().OnClose(func(w diskwriter.Writer, _ error) error {
		v.removeWriter(w.SliceKey())
		return nil
	})

	return w, nil
}

func (v *Volume) Writers() (out []diskwriter.Writer) {
	v.writersLock.Lock()
	defer v.writersLock.Unlock()

	out = make([]diskwriter.Writer, 0, len(v.writers))
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
