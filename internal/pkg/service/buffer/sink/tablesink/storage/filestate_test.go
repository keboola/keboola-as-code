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

func TestFile_StateTransition(t *testing.T) {
	t.Parallel()

	clk := clock.Mock{}
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Create file entity
	fileKey := testFileKey()

	var err error
	file := File{
		FileKey: fileKey,
		State:   FileWriting,
	}

	// FileClosing
	clk.Add(time.Hour)
	file, err = file.WithState(clk.Now(), FileClosing)
	require.NoError(t, err)
	assert.Equal(t, File{
		FileKey:   fileKey,
		State:     FileClosing,
		ClosingAt: ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
	}, file)

	// FileImporting
	clk.Add(time.Hour)
	file, err = file.WithState(clk.Now(), FileImporting)
	require.NoError(t, err)
	assert.Equal(t, File{
		FileKey:     fileKey,
		State:       FileImporting,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		ImportingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
	}, file)

	// FileImported
	clk.Add(time.Hour)
	file, err = file.WithState(clk.Now(), FileImported)
	require.NoError(t, err)
	assert.Equal(t, File{
		FileKey:     fileKey,
		State:       FileImported,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		ImportingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		ImportedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
	}, file)

	// Invalid
	if _, err = file.WithState(clk.Now(), FileImporting); assert.Error(t, err) {
		wildcards.Assert(t, `unexpected file "%s" state transition from "imported" to "importing"`, err.Error())
	}
}
