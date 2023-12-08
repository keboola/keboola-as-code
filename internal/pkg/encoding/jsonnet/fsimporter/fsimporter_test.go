package fsimporter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestImporter(t *testing.T) {
	t.Parallel()

	// Create context
	ctx := context.Background()
	fs := aferofs.NewMemoryFs()
	jsonnetCtx := jsonnet.NewContext().WithImporter(fsimporter.New(fs))

	// File is missing
	_, err := jsonnet.Evaluate(`import "abc.jsonnet"`, jsonnetCtx)
	assert.Error(t, err)
	assert.Equal(t, `jsonnet error: RUNTIME ERROR: missing file "abc.jsonnet"`, err.Error())

	// File is found
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("foo/bar/A.jsonnet", `import "B.jsonnet"`)))          // relative
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("foo/bar/B.jsonnet", `import "/foo/bar/C.jsonnet"`))) // absolute
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("foo/bar/C.jsonnet", `{some: "value"}`)))
	out, err := jsonnet.Evaluate(`import "foo/bar/A.jsonnet"`, jsonnetCtx)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"some\": \"value\"\n}\n", out)
}
