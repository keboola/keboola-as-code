package writer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/csv"
)

// TestDefaultFactory_FileTypeCSV tests that csv.Writer is created for the storage.FileTypeCSV.
// Test for csv.Writer itself are in the "csv" package.
func TestDefaultFactory_FileTypeCSV(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	v, err := tc.OpenVolume(WithWriterFactory(DefaultFactory))
	assert.NoError(t, err)

	slice := newTestSlice()
	slice.Type = storage.FileTypeCSV

	w, err := v.NewWriterFor(slice)
	assert.NoError(t, err)
	assert.NotNil(t, w)

	_, ok := w.(*csv.Writer)
	assert.True(t, ok)
}

// TestDefaultFactory_FileTypeInvalid test handling of an invalid file type.
func TestDefaultFactory_FileTypeInvalid(t *testing.T) {
	t.Parallel()
	tc := newVolumeTestCase(t)

	v, err := tc.OpenVolume(WithWriterFactory(DefaultFactory))
	assert.NoError(t, err)

	slice := newTestSlice()
	slice.Type = "invalid"
	_, err = v.NewWriterFor(slice)
	if assert.Error(t, err) {
		assert.Equal(t, `unexpected file type "invalid"`, err.Error())
	}
}
