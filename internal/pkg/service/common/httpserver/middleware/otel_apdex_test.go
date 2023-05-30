package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type testRequest struct {
	DurationMs float64
	StatusCode int
}

type testCase struct {
	Name               string
	Requests           []testRequest
	ExpectedApdexT500  float64
	ExpectedApdexT1000 float64
}

func TestApdex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []testCase{
		{
			Name:               "server error, apdex=0",
			Requests:           []testRequest{{DurationMs: 1, StatusCode: 500}, {DurationMs: 1, StatusCode: 503}},
			ExpectedApdexT500:  0,
			ExpectedApdexT1000: 0,
		},
		{
			Name:               "client error, satisfied duration",
			Requests:           []testRequest{{DurationMs: 1, StatusCode: 400}, {DurationMs: 1, StatusCode: 403}},
			ExpectedApdexT500:  1,
			ExpectedApdexT1000: 1,
		},
		{
			Name:               "ok, satisfied duration",
			Requests:           []testRequest{{DurationMs: 1, StatusCode: 200}, {DurationMs: 1, StatusCode: 201}},
			ExpectedApdexT500:  1,
			ExpectedApdexT1000: 1,
		},
		{
			Name:               "ok, tolerated/satisfied duration",
			Requests:           []testRequest{{DurationMs: 700, StatusCode: 200}, {DurationMs: 800, StatusCode: 201}},
			ExpectedApdexT500:  0.5, // T=500ms < 700ms,800ms < 4T=2000ms --> tolerated range --> apdex=0.5
			ExpectedApdexT1000: 1,   //           700ms,800ms <  T=1000ms --> satisfied range --> apdex=1
		},
		{
			Name:               "ok, frustrated/tolerated duration",
			Requests:           []testRequest{{DurationMs: 2400, StatusCode: 200}, {DurationMs: 2500, StatusCode: 201}},
			ExpectedApdexT500:  0,   // 2400ms,2500ms > 4T=2000ms            --> frustrated range --> apdex=0
			ExpectedApdexT1000: 0.5, // T=1000ms < 2400ms,2500ms < 4T=4000ms --> tolerated range  --> apdex=0.5
		},
		{
			Name:               "ok, frustrated duration",
			Requests:           []testRequest{{DurationMs: 10000, StatusCode: 200}, {DurationMs: 11000, StatusCode: 201}},
			ExpectedApdexT500:  0,
			ExpectedApdexT1000: 0,
		},
	}

	for _, tc := range cases {
		tel := telemetry.NewForTest(t)
		counters := apdexCounters(tel.MeterProvider().Meter("test"), []time.Duration{
			500 * time.Millisecond,
			1000 * time.Millisecond,
		})
		for _, req := range tc.Requests {
			counters.Record(ctx, req.DurationMs, req.StatusCode)
		}

		metrics := tel.Metrics(t, telemetry.WithKeepHistogramSum(true))
		assert.Len(t, metrics, 2)
		assert.Len(t, metrics[0].Data.(metricdata.Histogram[float64]).DataPoints, 1)
		assert.Len(t, metrics[1].Data.(metricdata.Histogram[float64]).DataPoints, 1)
		t500Data := metrics[0].Data.(metricdata.Histogram[float64]).DataPoints[0]
		t1000Data := metrics[1].Data.(metricdata.Histogram[float64]).DataPoints[0]
		assert.Equal(t, tc.ExpectedApdexT500, t500Data.Sum/float64(t500Data.Count))
		assert.Equal(t, tc.ExpectedApdexT1000, t1000Data.Sum/float64(t1000Data.Count))
	}
}
