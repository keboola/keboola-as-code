package worker

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestBufferWorkerE2E(t *testing.T) {
	t.Parallel()
	_, testFile, _, _ := runtime.Caller(0)
	testsDir := filepath.Dir(testFile) //nolint:forbidigo

	project := testproject.GetTestProjectForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Start API and Worker nodes
	ts := startCluster(t, ctx, testsDir, project)

	// Run test-cases
	ts.test000Setup()
	ts.test001ImportRecords()
	ts.test002SliceUpload()
	ts.test003FileImport()
	ts.test004EmptyFileAndSlice()
	ts.test998BufferSizeOverflow()
	ts.test999Cleanup()

	// Shutdown all nodes
	ts.Shutdown()
}
