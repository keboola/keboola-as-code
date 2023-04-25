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
	if true {
		t.Skipf("TMP skip")
	}

	t.Parallel()
	_, testFile, _, _ := runtime.Caller(0)
	testsDir := filepath.Dir(testFile) //nolint:forbidigo

	project := testproject.GetTestProjectForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	ts := newTestSuite(t, ctx, testsDir, project)
	go func() {
		// Start API and Worker nodes
		ts.StartCluster()

		// Run test-cases
		ts.test000Setup()
		ts.test001ImportRecords()
		ts.test002SliceUpload()
		ts.test003FileImport()
		ts.test004EmptyFileAndSlice()
		ts.test998BufferSizeOverflow()
		ts.test999Cleanup()

		// Shutdown all nodes
		ts.ShutdownCluster()
	}()

	// t.Fatal cannot be called from a goroutine, it would not stop the test, therefore the fatalCh is used.
	// https://github.com/golang/go/issues/15758
	if err := <-ts.fatalCh; err != nil {
		t.Fatal(err)
	}
}
