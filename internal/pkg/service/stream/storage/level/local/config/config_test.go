package config_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[config.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "volume.assignment.count" is a required field
- "volume.assignment.preferredTypes" is a required field
- "volume.registration.ttlSeconds" is a required field
- "encoding.encoder.type" is a required field
- "encoding.encoder.rowSizeLimit" must be 1KB or greater
- "encoding.maxChunkSize" is a required field
- "encoding.failedChunksThreshold" is a required field
- "encoding.compression.type" is a required field
- "encoding.sync.mode" is a required field
- "encoding.sync.checkInterval" is a required field
- "encoding.sync.countTrigger" is a required field
- "encoding.sync.uncompressedBytesTrigger" is a required field
- "encoding.sync.compressedBytesTrigger" is a required field
- "encoding.sync.intervalTrigger" is a required field
- "writer.network.listen" is a required field
- "writer.network.transport" is a required field
- "writer.network.keepAliveInterval" is a required field
- "writer.network.minSlicesPerSourceNode" is a required field
- "writer.network.maxWaitingStreamsPerConn" is a required field
- "writer.network.streamMaxWindow" is a required field
- "writer.network.streamOpenTimeout" is a required field
- "writer.network.streamCloseTimeout" is a required field
- "writer.network.streamWriteTimeout" is a required field
- "writer.network.shutdownTimeout" is a required field
- "writer.network.kcpInputBuffer" is a required field
- "writer.network.kcpResponseBuffer" is a required field
- "writer.network.pipelineBalancer" is a required field
- "writer.allocation.static" is a required field
- "writer.allocation.relative" must be 100 or greater
`,
			Value: config.Config{},
		},
		{
			Name:  "default",
			Value: config.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
