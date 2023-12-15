package storage

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSlice_StateTransition(t *testing.T) {
	t.Parallel()

	clk := clock.Mock{}
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Create file entity
	sliceKey := testSliceKey()
	f := Slice{
		SliceKey: sliceKey,
		State:    SliceWriting,
	}

	// SliceClosing
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), SliceClosing))
	assert.Equal(t, Slice{
		SliceKey:  sliceKey,
		State:     SliceClosing,
		ClosingAt: ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
	}, f)

	// SliceUploading
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), SliceUploading))
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploading,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
	}, f)

	// SliceUploaded
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), SliceUploaded))
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploaded,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
	}, f)

	// SliceImported
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), SliceImported))
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceImported,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
		ImportedAt:  ptr(utctime.MustParse("2000-01-01T04:00:00.000Z")),
	}, f)

	// Invalid
	if err := f.StateTransition(clk.Now(), SliceUploaded); assert.Error(t, err) {
		wildcards.Assert(t, `unexpected slice "%s" state transition from "imported" to "uploaded"`, err.Error())
	}
}
