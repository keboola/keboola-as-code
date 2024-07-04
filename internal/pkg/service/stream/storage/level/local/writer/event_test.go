package writer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/test"
	writerVolume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestEventWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.New()
	process := servicectx.New()

	// There are 2 volumes
	volumesPath := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "1"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "2"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", volume.IDFile), []byte("HDD_1"), 0o640))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "2", volume.IDFile), []byte("HDD_2"), 0o640))

	// Detect volumes
	volumes, err := writerVolume.OpenVolumes(ctx, logger, clk, process, "my-node", volumesPath, writer.NewConfig(), writerVolume.WithFormatWriterFactory(test.DummyWriterFactory))
	assert.NoError(t, err)

	// Register "OnWriterOpen" and "OnWriterClose" events on the "volumes" level
	volumes.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (1), level: volumes`, w.SliceKey().OpenedAt())
		return nil
	})
	volumes.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (2), level: volumes`, w.SliceKey().OpenedAt())
		return nil
	})
	volumes.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (1), level: volumes`, w.SliceKey().OpenedAt())
		return nil
	})
	volumes.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (2), level: volumes`, w.SliceKey().OpenedAt())
		return nil
	})

	// Register "OnWriterOpen" and "OnWriterClose" events on the "volume" level
	vol1, err := volumes.Collection().Volume("HDD_1")
	assert.NoError(t, err)
	vol2, err := volumes.Collection().Volume("HDD_2")
	assert.NoError(t, err)
	vol1.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (3), level: volume1`, w.SliceKey().OpenedAt())
		return nil
	})
	vol1.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (4), level: volume1`, w.SliceKey().OpenedAt())
		return nil
	})
	vol1.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (3), level: volume1`, w.SliceKey().OpenedAt())
		return nil
	})
	vol1.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (4), level: volume1`, w.SliceKey().OpenedAt())
		return nil
	})
	vol2.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (3), level: volume2`, w.SliceKey().OpenedAt())
		return nil
	})
	vol2.Events().OnWriterOpen(func(w writer.Writer) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: OPEN (4), level: volume2`, w.SliceKey().OpenedAt())
		return nil
	})
	vol2.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (3), level: volume2`, w.SliceKey().OpenedAt())
		return nil
	})
	vol2.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (4), level: volume2`, w.SliceKey().OpenedAt())
		return nil
	})

	// Register "OnWriterClose" event on the "writer" level
	slice1 := test.NewSliceOpenedAt("2001-01-01T00:00:00.000Z")
	slice2 := test.NewSliceOpenedAt("2002-01-01T00:00:00.000Z")
	writer1, err := vol1.OpenWriter(slice1)
	require.NoError(t, err)
	writer2, err := vol2.OpenWriter(slice2)
	require.NoError(t, err)
	writer1.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (5), level: writer1`, w.SliceKey().OpenedAt())
		return nil
	})
	writer1.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (6), level: writer1`, w.SliceKey().OpenedAt())
		return nil
	})
	writer2.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (5), level: writer2`, w.SliceKey().OpenedAt())
		return nil
	})
	writer2.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		logger.Infof(ctx, `EVENT: slice: "%s", event: CLOSE (6), level: writer2`, w.SliceKey().OpenedAt())
		return nil
	})

	// Close all
	process.Shutdown(ctx, errors.New("bye bye"))
	process.WaitForShutdown()

	// Check logs, closing is parallel, so writers logs are checked separately
	logger.AssertJSONMessages(t, `
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: OPEN (4), level: volume1"}                    
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: OPEN (3), level: volume1"}                    
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: OPEN (2), level: volumes"}                    
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: OPEN (1), level: volumes"} 
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (6), level: writer1"}
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (5), level: writer1"}
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (4), level: volume1"}
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (3), level: volume1"}
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (2), level: volumes"}
{"level":"info","message":"EVENT: slice: \"2001-01-01T00:00:00.000Z\", event: CLOSE (1), level: volumes"}
`)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: OPEN (4), level: volume2"}                    
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: OPEN (3), level: volume2"}                    
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: OPEN (2), level: volumes"}                    
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: OPEN (1), level: volumes"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (6), level: writer2"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (5), level: writer2"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (4), level: volume2"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (3), level: volume2"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (2), level: volumes"}
{"level":"info","message":"EVENT: slice: \"2002-01-01T00:00:00.000Z\", event: CLOSE (1), level: volumes"}
`)
}

func TestWriterEvents_OpenError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.New()
	process := servicectx.New()

	// There are 2 volumes
	volumesPath := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "1"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", volume.IDFile), []byte("HDD_1"), 0o640))

	// Detect volumes
	volumes, err := writerVolume.OpenVolumes(ctx, logger, clk, process, "my-node", volumesPath, writer.NewConfig(), writerVolume.WithFormatWriterFactory(test.DummyWriterFactory))
	assert.NoError(t, err)

	// Register "OnWriterOpen" event on the "volumes" level
	volumes.Events().OnWriterOpen(func(w writer.Writer) error {
		return errors.New("error (1)")
	})

	// Register "OnWriterOpen" event on the "volume" level
	vol, err := volumes.Collection().Volume("HDD_1")
	assert.NoError(t, err)
	vol.Events().OnWriterOpen(func(w writer.Writer) error {
		return errors.New("error (2)")
	})

	// Check error
	_, err = vol.OpenWriter(test.NewSlice())
	if assert.Error(t, err) {
		assert.Equal(t, "- error (2)\n- error (1)", err.Error())
	}

	// Close volumes
	process.Shutdown(ctx, errors.New("bye bye"))
	process.WaitForShutdown()
}

func TestEventWriter_CloseError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.New()
	process := servicectx.New()

	// There are 2 volumes
	volumesPath := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(volumesPath, "hdd", "1"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "1", volume.IDFile), []byte("HDD_1"), 0o640))

	// Detect volumes
	volumes, err := writerVolume.OpenVolumes(ctx, logger, clk, process, "my-node", volumesPath, writer.NewConfig(), writerVolume.WithFormatWriterFactory(test.DummyWriterFactory))
	assert.NoError(t, err)

	// Register "OnWriterClose" event on the "volumes" level
	volumes.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		return errors.New("error (1)")
	})

	// Register "OnWriterClose" event on the "volume" level
	vol, err := volumes.Collection().Volume("HDD_1")
	assert.NoError(t, err)
	vol.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		return errors.New("error (2)")
	})

	// Create writer
	w, err := vol.OpenWriter(test.NewSlice())
	require.NoError(t, err)

	// Register "OnWriterClose" event on the "writer" level
	w.Events().OnWriterClose(func(w writer.Writer, _ error) error {
		return errors.New("error (3)")
	})

	// Check error
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "- error (3)\n- error (2)\n- error (1)", err.Error())
	}

	// Close volumes
	process.Shutdown(ctx, errors.New("bye bye"))
	process.WaitForShutdown()
}
