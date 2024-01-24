package local_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultCfg := local.NewConfig()

	// Apply empty patch
	assert.Equal(t, defaultCfg, defaultCfg.With(local.ConfigPatch{}))

	// Apply full patch
	patchedCfg := defaultCfg.With(local.ConfigPatch{
		Compression: &compression.ConfigPatch{
			GZIP: &compression.GZIPConfigPatch{
				Level:          test.Ptr(3),
				Implementation: test.Ptr(compression.GZIPImplStandard),
			},
		},
		DiskSync: &disksync.ConfigPatch{
			Mode: test.Ptr(disksync.ModeCache),
			Wait: test.Ptr(false),
		},
		DiskAllocation: &diskalloc.ConfigPatch{
			SizePercent: test.Ptr(123),
		},
	})
	expectedCfg := defaultCfg
	expectedCfg.Compression.GZIP.Level = 3
	expectedCfg.Compression.GZIP.Implementation = compression.GZIPImplStandard
	expectedCfg.DiskSync.Mode = disksync.ModeCache
	expectedCfg.DiskSync.Wait = false
	expectedCfg.DiskAllocation.SizePercent = 123
	assert.Equal(t, expectedCfg, patchedCfg)
}

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[local.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "compression.type" is a required field
- "diskSync.mode" is a required field
- "diskAllocation.size" is a required field
- "diskAllocation.sizePercent" must be 100 or greater
`,
			Value: local.Config{},
		},
		{
			Name:  "default",
			Value: local.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
