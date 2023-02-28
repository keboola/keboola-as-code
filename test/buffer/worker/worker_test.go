package worker

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
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
	c := startCluster(t, ctx, testsDir, project)

	// Create receiver with two exports
	receiver := c.CreateReceiver(t, "my-receiver")
	secret := receiver.URL[strings.LastIndex(receiver.URL, "/")+1:]
	export1 := c.CreateExport(t, receiver, "my-export-1",
		&buffer.Column{Name: "idCol", Type: "id", PrimaryKey: true},
		&buffer.Column{Name: "dateCol", Type: "datetime"},
		&buffer.Column{Name: "bodyCol", Type: "body"},
		&buffer.Column{Name: "headersCol", Type: "headers"},
	)
	export2 := c.CreateExport(t, receiver, "my-export-2",
		&buffer.Column{Name: "dateCol", Type: "datetime", PrimaryKey: true},
		&buffer.Column{Name: "keyCol", Type: "template", Template: &buffer.Template{Language: "jsonnet", Content: `"---" + Body("key") + "---"`}},
	)

	assert.NotEmpty(t, secret)
	assert.NotEmpty(t, export1)
	assert.NotEmpty(t, export2)

	// Check initial state
	c.AssertEtcdState(t, "000-setup")

	c.Shutdown()
}
