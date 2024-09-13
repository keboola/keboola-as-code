package balancer_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRoundRobinBalancer(t *testing.T) {
	t.Parallel()

	// Fixtures
	b := balancer.NewRoundRobinBalancer()
	c := recordctx.FromHTTP(time.Now(), &http.Request{})

	// Pipelines
	var logger strings.Builder
	p1 := NewTestPipeline("pipeline1", test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z"), &logger)
	p2 := NewTestPipeline("pipeline2", test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z"), &logger)
	p3 := NewTestPipeline("pipeline3", test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z"), &logger)
	pipelines := []balancer.SlicePipeline{p1, p2, p3}
	expectWriteToPipeline := func(expectedPipeline *TestPipeline) {
		result, err := b.WriteRecord(c, pipelines)
		assert.Equal(t, pipeline.RecordProcessed, result.Status)
		assert.NoError(t, err)
		assert.Equal(t, "write "+expectedPipeline.Name, strings.TrimSpace(logger.String()))
		logger.Reset()
	}
	expectWriteError := func(expectedErr string) {
		result, err := b.WriteRecord(c, pipelines)
		assert.Equal(t, pipeline.RecordError, result.Status)
		if assert.Error(t, err) {
			assert.Equal(t, expectedErr, err.Error())
		}
	}

	// No pipeline
	result, err := b.WriteRecord(c, nil)
	assert.Equal(t, pipeline.RecordError, result.Status)
	if assert.Error(t, err) {
		assert.Equal(t, "no pipeline", err.Error())
	}

	// Simple - all pipelines are ready
	expectWriteToPipeline(p1)
	expectWriteToPipeline(p2)
	expectWriteToPipeline(p3)

	// Disable pipelines, up to "no pipeline ready"
	p1.Ready = false
	expectWriteToPipeline(p2)
	p1.Ready = true
	p3.Ready = false
	// 4%3 == 1 (p2)
	expectWriteToPipeline(p2)
	// 5%3 == 2 (p3) Not ready, use p1 as RR
	expectWriteToPipeline(p1)

	p1.Ready = false
	p2.Ready = false
	expectWriteError("no pipeline is ready")

	// Write error
	p3.Ready = true
	p3.WriteError = errors.New("some write error")
	expectWriteError("some write error")
}
