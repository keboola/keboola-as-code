package storage

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestSlice_StateTransition(t *testing.T) {
	t.Parallel()

	clk := clock.Mock{}
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Create file entity
	sliceKey := testSliceKey()

	var err error
	slice := Slice{
		SliceKey: sliceKey,
		State:    SliceWriting,
	}

	// SliceClosing
	clk.Add(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceClosing)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:  sliceKey,
		State:     SliceClosing,
		ClosingAt: ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
	}, slice)

	// SliceUploading
	clk.Add(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceUploading)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploading,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
	}, slice)

	// SliceUploaded
	clk.Add(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceUploaded)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploaded,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
	}, slice)

	// SliceImported
	clk.Add(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceImported)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceImported,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
		ImportedAt:  ptr(utctime.MustParse("2000-01-01T04:00:00.000Z")),
	}, slice)

	// Invalid
	if _, err = slice.WithState(clk.Now(), SliceUploaded); assert.Error(t, err) {
		wildcards.Assert(t, `unexpected slice "%s" state transition from "imported" to "uploaded"`, err.Error())
	}
}
