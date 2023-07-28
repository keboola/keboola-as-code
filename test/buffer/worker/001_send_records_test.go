package worker

import (
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

// test001SendPayload sends 4 records; 4 < 5 (uploadCountThreshold) - upload is not triggered.
func (ts *testSuite) test001SendPayload() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("001 send records")
	ts.t.Logf("-------------------------")

	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][bufferWorker][conditions]DEBUG checked "2" opened slices | %s
	`)

	// Send records 1-4
	for i := 1; i <= 4; i++ {
		ts.SendPayload(i)
	}

	// Check API metrics
	recordsMetric := atomic.NewFloat64(0)
	bodyBytesMetric := atomic.NewFloat64(0)
	ts.IterateMetrics(func(ch <-chan *dto.MetricFamily) {
		for f := range ch {
			switch *f.Name {
			case "keboola_go_buffer_ingress_records_total":
				for _, m := range f.Metric {
					recordsMetric.Add(m.Counter.GetValue())
				}
			case "keboola_go_buffer_ingress_bytes_total":
				for _, m := range f.Metric {
					bodyBytesMetric.Add(m.Counter.GetValue())
				}
			}
		}
	})
	assert.Equal(ts.t, float64(4), recordsMetric.Load())
	assert.Equal(ts.t, float64(84), bodyBytesMetric.Load())

	// Check etcd state
	// Statistics are ignored, because they are tracked per API node and for each request is used a random API node.
	ts.AssertEtcdState("001-send-payload", etcdhelper.WithIgnoredKeyPattern(`^stats/`))
}
