package schema_test

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
)

func TestRecordKey_String(t *testing.T) {
	t.Parallel()

	now, err := time.Parse(time.RFC3339, `2006-01-02T15:04:05+07:00`)
	assert.NoError(t, err)

	key := schema.RecordKey{
		ProjectID:  1000,
		ReceiverID: "my-receiver",
		ExportID:   "my-export",
		FileID:     "file456",
		SliceID:    "slice789",
		ReceivedAt: now,
	}
	wildcards.Assert(t, "record/1000/my-receiver/my-export/file456/slice789/2006-01-02T08:04:05.000Z_%c%c%c%c%c", key.In(schema.New(noValidation)).Key())
}
