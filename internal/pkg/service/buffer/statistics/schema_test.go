package statistics

import (
	"fmt"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := newSchema(serde.NewJSON(serde.NoValidation))

	time1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	time2 := time1.Add(time.Hour)

	projectID := keboola.ProjectID(123)
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(time1)}
	sliceKey := key.SliceKey{SliceID: key.SliceID(time2), FileKey: fileKey}
	nodeID := "my-node"

	cases := []keyTestCase{
		{
			s.InCategory(Buffered).Prefix(),
			"stats/buffered/",
		},
		{
			s.InCategory(Uploaded).Prefix(),
			"stats/uploaded/",
		},
		{
			s.InCategory(Imported).Prefix(),
			"stats/imported/",
		},
		{
			s.InCategory(Buffered).InReceiver(receiverKey).Prefix(),
			"stats/buffered/123/my-receiver/",
		},
		{
			s.InCategory(Buffered).InExport(exportKey).Prefix(),
			"stats/buffered/123/my-receiver/my-export/",
		},
		{
			s.InCategory(Buffered).InExport(exportKey).CleanupSum().Key(),
			"stats/buffered/123/my-receiver/my-export/_cleanup_sum",
		},
		{
			s.InCategory(Buffered).InFile(fileKey).Prefix(),
			"stats/buffered/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.InCategory(Buffered).InSlice(sliceKey).Prefix(),
			"stats/buffered/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/",
		},
		{
			s.InCategory(Buffered).InSlice(sliceKey).PerNode(nodeID).Key(),
			"stats/buffered/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/my-node",
		},
		{
			s.InCategory(Buffered).InSlice(sliceKey).NodesSum().Key(),
			"stats/buffered/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/_nodes_sum",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}
