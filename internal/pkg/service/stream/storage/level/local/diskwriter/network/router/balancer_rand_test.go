package router_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRandomBalancer(t *testing.T) {
	t.Parallel()

	// Fixtures
	randomizer := router.NewTestRandomizer()
	balancer := router.NewRandomBalancerWithRandomizer(randomizer)
	c := recordctx.FromHTTP(time.Now(), &http.Request{})

	// Pipelines
	var logger strings.Builder
	p1 := NewTestPipeline("pipeline1", test.NewSliceKeyOpenedAt("2000-01-01:01:00.000Z"), &logger)
	p2 := NewTestPipeline("pipeline2", test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z"), &logger)
	p3 := NewTestPipeline("pipeline3", test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z"), &logger)
	pipelines := []router.SlicePipeline{p1, p2, p3}
	expectWriteToPipeline := func(expectedPipeline *TestPipeline) {
		status, err := balancer.WriteRecord(c, pipelines)
		assert.Equal(t, status, pipeline.RecordProcessed)
		assert.NoError(t, err)
		assert.Equal(t, "write "+expectedPipeline.Name, strings.TrimSpace(logger.String()))
		logger.Reset()
	}
	expectWriteError := func(expectedErr string) {
		status, err := balancer.WriteRecord(c, pipelines)
		assert.Equal(t, status, pipeline.RecordError)
		if assert.Error(t, err) {
			assert.Equal(t, expectedErr, err.Error())
		}
	}

	// No pipeline
	status, err := balancer.WriteRecord(c, nil)
	assert.Equal(t, pipeline.RecordError, status)
	if assert.Error(t, err) {
		assert.Equal(t, "no pipeline", err.Error())
	}

	// Simple - all pipelines are ready
	randomizer.QueueIntN(1) // index 1 == pipeline 2
	expectWriteToPipeline(p2)
	randomizer.QueueIntN(0)
	expectWriteToPipeline(p1)
	randomizer.QueueIntN(2)
	expectWriteToPipeline(p3)

	// Disable pipelines, up to "no pipeline ready"
	p2.Ready = false
	randomizer.QueueIntN(1)
	expectWriteToPipeline(p3)
	p3.Ready = false
	randomizer.QueueIntN(0)
	expectWriteToPipeline(p1)
	p1.Ready = false
	randomizer.QueueIntN(2)
	expectWriteError("no pipeline is ready")

	// Write error
	p3.Ready = true
	p3.WriteError = errors.New("some write error")
	randomizer.QueueIntN(1)
	expectWriteError("some write error")
}
