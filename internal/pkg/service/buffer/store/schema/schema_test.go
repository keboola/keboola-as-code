package schema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := schema.New(noValidation)

	time1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	time2 := time1.Add(time.Hour)

	projectID := 123
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	mappingKey := key.MappingKey{ExportKey: exportKey, RevisionID: 10}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: time1}
	sliceKey := key.SliceKey{SliceID: time2, FileKey: fileKey}

	cases := []keyTestCase{
		{
			s.Configs().Prefix(),
			"config/",
		},
		{
			s.Configs().Receivers().Prefix(),
			"config/receiver/",
		},
		{
			s.Configs().Receivers().InProject(projectID).Prefix(),
			"config/receiver/123/",
		},
		{
			s.Configs().Receivers().ByKey(receiverKey).Key(),
			"config/receiver/123/my-receiver",
		},
		{
			s.Configs().Exports().Prefix(),
			"config/export/",
		},
		{
			s.Configs().Exports().InReceiver(receiverKey).Prefix(),
			"config/export/123/my-receiver/",
		},
		{
			s.Configs().Exports().ByKey(exportKey).Key(),
			"config/export/123/my-receiver/my-export",
		},
		{
			s.Configs().Mappings().Prefix(),
			"config/mapping/revision/",
		},
		{
			s.Configs().Mappings().InReceiver(receiverKey).Prefix(),
			"config/mapping/revision/123/my-receiver/",
		},
		{
			s.Configs().Mappings().InExport(exportKey).Prefix(),
			"config/mapping/revision/123/my-receiver/my-export/",
		},
		{
			s.Configs().Mappings().ByKey(mappingKey).Key(),
			"config/mapping/revision/123/my-receiver/my-export/00000010",
		},
		{
			s.Secrets().Tokens().InReceiver(receiverKey).Prefix(),
			"secret/export/token/123/my-receiver/",
		},
		{
			s.Secrets().Tokens().InExport(exportKey).Key(),
			"secret/export/token/123/my-receiver/my-export",
		},

		{
			s.Files().Prefix(),
			"file/",
		},
		{
			s.Files().Opened().Prefix(),
			"file/opened/",
		},
		{
			s.Files().Opened().InExport(exportKey).Prefix(),
			"file/opened/123/my-receiver/my-export/",
		},
		{
			s.Files().Opened().ByKey(fileKey).Key(),
			"file/opened/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Closing().Prefix(),
			"file/closing/",
		},
		{
			s.Files().Closing().InExport(exportKey).Prefix(),
			"file/closing/123/my-receiver/my-export/",
		},
		{
			s.Files().Closing().ByKey(fileKey).Key(),
			"file/closing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Closed().Prefix(),
			"file/closed/",
		},
		{
			s.Files().Closed().InExport(exportKey).Prefix(),
			"file/closed/123/my-receiver/my-export/",
		},
		{
			s.Files().Closed().ByKey(fileKey).Key(),
			"file/closed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Importing().Prefix(),
			"file/importing/",
		},
		{
			s.Files().Importing().InExport(exportKey).Prefix(),
			"file/importing/123/my-receiver/my-export/",
		},
		{
			s.Files().Importing().ByKey(fileKey).Key(),
			"file/importing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Imported().Prefix(),
			"file/imported/",
		},
		{
			s.Files().Imported().InExport(exportKey).Prefix(),
			"file/imported/123/my-receiver/my-export/",
		},
		{
			s.Files().Imported().ByKey(fileKey).Key(),
			"file/imported/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Failed().Prefix(),
			"file/failed/",
		},
		{
			s.Files().Failed().InExport(exportKey).Prefix(),
			"file/failed/123/my-receiver/my-export/",
		},
		{
			s.Files().Failed().ByKey(fileKey).Key(),
			"file/failed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Slices().Prefix(),
			"slice/",
		},
		{
			s.Slices().Opened().Prefix(),
			"slice/opened/",
		},
		{
			s.Slices().Opened().InFile(fileKey).Prefix(),
			"slice/opened/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Opened().ByKey(sliceKey).Key(),
			"slice/opened/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Slices().Closing().InFile(fileKey).Prefix(),
			"slice/closing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Closing().ByKey(sliceKey).Key(),
			"slice/closing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Slices().Closed().InFile(fileKey).Prefix(),
			"slice/closed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Closed().ByKey(sliceKey).Key(),
			"slice/closed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Slices().Uploading().InFile(fileKey).Prefix(),
			"slice/uploading/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Uploading().ByKey(sliceKey).Key(),
			"slice/uploading/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Slices().Uploaded().InFile(fileKey).Prefix(),
			"slice/uploaded/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Uploaded().ByKey(sliceKey).Key(),
			"slice/uploaded/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Slices().Failed().InFile(fileKey).Prefix(),
			"slice/failed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().Failed().ByKey(sliceKey).Key(),
			"slice/failed/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Records().Prefix(),
			"record/",
		},
		{
			s.Records().ByKey(key.RecordKey{
				ReceivedAt:   time2,
				RandomSuffix: "abcdef",
				ExportKey: key.ExportKey{
					ExportID: "my-export",
					ReceiverKey: key.ReceiverKey{
						ProjectID:  123,
						ReceiverID: "my-receiver",
					},
				},
				SliceID: time1,
			}).Key(),
			"record/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z_abcdef",
		},
		{
			s.Secrets().Tokens().InExport(key.ExportKey{
				ReceiverKey: key.ReceiverKey{
					ProjectID:  123,
					ReceiverID: "my-receiver",
				},
				ExportID: "my-export",
			}).Key(),
			"secret/export/token/123/my-receiver/my-export",
		},
		{
			s.Runtime().Prefix(),
			"runtime/",
		},
		{
			s.Runtime().Workers().Prefix(),
			"runtime/workers/",
		},
		{
			s.Runtime().Workers().Active().Prefix(),
			"runtime/workers/active/",
		},
		{
			s.Runtime().Workers().Active().IDs().Prefix(),
			"runtime/workers/active/ids/",
		},
		{
			s.Runtime().Workers().Active().IDs().Node("my-node").Key(),
			"runtime/workers/active/ids/my-node",
		},
		{
			s.SliceStats().ByKey(key.SliceStatsKey{
				SliceKey: sliceKey,
				NodeID:   "my-node",
			}).Key(),
			"stats/received/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/my-node",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func noValidation(_ context.Context, _ any) error {
	return nil
}
