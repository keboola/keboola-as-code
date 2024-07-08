package manager

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type writerRef struct {
	encoding.Writer
}

func (m *Manager) OpenWriter(ctx context.Context, slice *model.Slice) (w encoding.Writer, err error) {

	// Check if the writer already exists, if not, register an empty reference to unlock immediately
	ref, exists := m.addWriter(slice.SliceKey)
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
		m.removeWriter(slice.SliceKey)
	}()

	// Open output
	out, err := m.config.OutputOpener(slice.SliceKey)
	if err != nil {
		return nil, err
	}

	// Create writer
	w, err = encoding.NewWriter(ctx, m.logger, m.clock, m.config, slice, out, m.writerEvents)
	if err != nil {
		return nil, err
	}

	// Register writer close callback
	w.Events().OnClose(func(w encoding.Writer, _ error) error {
		m.removeWriter(w.SliceKey())
		return nil
	})

	return w, nil
}

func (m *Manager) addWriter(k model.SliceKey) (ref *writerRef, exists bool) {
	m.writersLock.Lock()
	defer m.writersLock.Unlock()

	key := k.String()
	ref, exists = m.writers[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple writers without being blocked by the lock.
		ref = &writerRef{}
		m.writers[key] = ref
	}

	return ref, exists
}

func (m *Manager) removeWriter(k model.SliceKey) {
	m.writersLock.Lock()
	defer m.writersLock.Unlock()
	delete(m.writers, k.String())
}
