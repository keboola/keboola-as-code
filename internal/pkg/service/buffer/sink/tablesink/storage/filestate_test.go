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

func TestFile_StateTransition(t *testing.T) {
	t.Parallel()

	clk := clock.Mock{}
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Create file entity
	fileKey := testFileKey()
	f := File{
		FileKey: fileKey,
		State:   FileWriting,
	}

	// FileClosing
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), FileClosing))
	assert.Equal(t, File{
		FileKey:   fileKey,
		State:     FileClosing,
		ClosingAt: ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
	}, f)

	// FileImporting
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), FileImporting))
	assert.Equal(t, File{
		FileKey:     fileKey,
		State:       FileImporting,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		ImportingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
	}, f)

	// FileImported
	clk.Add(time.Hour)
	require.NoError(t, f.StateTransition(clk.Now(), FileImported))
	assert.Equal(t, File{
		FileKey:     fileKey,
		State:       FileImported,
		ClosingAt:   ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		ImportingAt: ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		ImportedAt:  ptr(utctime.MustParse("2000-01-01T03:00:00.000Z")),
	}, f)

	// Invalid
	if err := f.StateTransition(clk.Now(), FileImporting); assert.Error(t, err) {
		wildcards.Assert(t, `unexpected file "%s" state transition from "imported" to "importing"`, err.Error())
	}
}
