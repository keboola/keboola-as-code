package model

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestSlice_StateTransition(t *testing.T) {
	t.Parallel()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Create file entity
	sliceKey := testSliceKey()

	var err error
	slice := Slice{
		SliceKey: sliceKey,
		State:    SliceWriting,
	}

	// SliceClosing
	clk.Advance(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceClosing)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:  sliceKey,
		State:     SliceClosing,
		ClosingAt: ptr.Ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
	}, slice)

	// SliceUploading
	clk.Advance(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceUploading)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploading,
		ClosingAt:   ptr.Ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
	}, slice)

	// SliceUploaded
	clk.Advance(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceUploaded)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceUploaded,
		ClosingAt:   ptr.Ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
	}, slice)

	// SliceImported
	clk.Advance(time.Hour)
	slice, err = slice.WithState(clk.Now(), SliceImported)
	require.NoError(t, err)
	assert.Equal(t, Slice{
		SliceKey:    sliceKey,
		State:       SliceImported,
		ClosingAt:   ptr.Ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		UploadingAt: ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		UploadedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
		ImportedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T04:00:00.000Z")),
	}, slice)

	// Invalid
	if _, err = slice.WithState(clk.Now(), SliceUploaded); assert.Error(t, err) {
		wildcards.Assert(t, `unexpected slice "%s" state transition from "imported" to "uploaded"`, err.Error())
	}
}

func TestSliceState_ToLevel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, LevelLocal, SliceWriting.Level())
	assert.Equal(t, LevelLocal, SliceClosing.Level())
	assert.Equal(t, LevelLocal, SliceUploading.Level())
	assert.Equal(t, LevelStaging, SliceUploaded.Level())
	assert.Equal(t, LevelTarget, SliceImported.Level())
	assert.PanicsWithError(t, `unexpected slice state "foo"`, func() {
		SliceState("foo").Level()
	})
}
