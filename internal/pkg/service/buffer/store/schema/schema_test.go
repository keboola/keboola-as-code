package schema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := schema.New(noValidation)

	time1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	time2 := time1.Add(time.Hour)

	projectID := keboola.ProjectID(123)
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	mappingKey := key.MappingKey{ExportKey: exportKey, RevisionID: 10}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(time1)}
	sliceKey := key.SliceKey{SliceID: key.SliceID(time2), FileKey: fileKey}
	recordKey := key.RecordKey{SliceKey: sliceKey, ReceivedAt: key.ReceivedAt(time2.Add(time.Hour)), RandomSuffix: "abcdef"}
	createdAt := utctime.UTCTime(time1)
	taskID := task.ID(fmt.Sprintf("%s/%s/%s_%s", receiverKey.ReceiverID.String(), "some.task", createdAt.String(), "abcdef"))
	taskKey := task.Key{ProjectID: projectID, TaskID: taskID}

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
			s.Files().InState(filestate.Opened).InReceiver(receiverKey).Prefix(),
			"file/opened/123/my-receiver/",
		},
		{
			s.Files().InState(filestate.Opened).InExport(exportKey).Prefix(),
			"file/opened/123/my-receiver/my-export/",
		},
		{
			s.Files().Opened().ByKey(fileKey).Key(),
			"file/opened/123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
		},
		{
			s.Files().Opened().Prefix(),
			"file/opened/",
		},
		{
			s.Files().Closing().Prefix(),
			"file/closing/",
		},
		{
			s.Files().Imported().Prefix(),
			"file/imported/",
		},
		{
			s.Files().Failed().Prefix(),
			"file/failed/",
		},
		{
			s.Slices().Prefix(),
			"slice/",
		},
		{
			s.Slices().AllActive().Prefix(),
			"slice/active/",
		},
		{
			s.Slices().AllArchived().Prefix(),
			"slice/archived/",
		},
		{
			s.Slices().AllOpened().Prefix(),
			"slice/active/opened/",
		},
		{
			s.Slices().AllClosed().Prefix(),
			"slice/active/closed/",
		},
		{
			s.Slices().AllSuccessful().Prefix(),
			"slice/archived/successful/",
		},
		{
			s.Slices().Writing().Prefix(),
			"slice/active/opened/writing/",
		},
		{
			s.Slices().Closing().Prefix(),
			"slice/active/opened/closing/",
		},
		{
			s.Slices().Uploading().Prefix(),
			"slice/active/closed/uploading/",
		},
		{
			s.Slices().Failed().Prefix(),
			"slice/active/closed/failed/",
		},
		{
			s.Slices().Uploaded().Prefix(),
			"slice/active/closed/uploaded/",
		},
		{
			s.Slices().Imported().Prefix(),
			"slice/archived/successful/imported/",
		},
		{
			s.Slices().InState(slicestate.Writing).Prefix(),
			"slice/active/opened/writing/",
		},
		{
			s.Slices().InState(slicestate.Writing).InReceiver(receiverKey).Prefix(),
			"slice/active/opened/writing/123/my-receiver/",
		},
		{
			s.Slices().InState(slicestate.Writing).InExport(exportKey).Prefix(),
			"slice/active/opened/writing/123/my-receiver/my-export/",
		},
		{
			s.Slices().InState(slicestate.Writing).InFile(fileKey).Prefix(),
			"slice/active/opened/writing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().InState(slicestate.Writing).ByKey(sliceKey).Key(),
			"slice/active/opened/writing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Records().Prefix(),
			"record/",
		},
		{
			s.Records().InReceiver(receiverKey).Prefix(),
			"record/123/my-receiver/",
		},
		{
			s.Records().InSlice(sliceKey).Prefix(),
			"record/123/my-receiver/my-export/2006-01-02T09:04:05.000Z/",
		},
		{
			s.Records().ByKey(recordKey).Key(),
			"record/123/my-receiver/my-export/2006-01-02T09:04:05.000Z/2006-01-02T10:04:05.000Z_abcdef",
		},
		{
			s.Secrets().Tokens().InExport(exportKey).Key(),
			"secret/export/token/123/my-receiver/my-export",
		},
		{
			s.Runtime().Prefix(),
			"runtime/",
		},
		{
			s.Runtime().APINodes().Prefix(),
			"runtime/api/node/",
		},
		{
			s.Runtime().APINodes().Watchers().Prefix(),
			"runtime/api/node/watcher/",
		},
		{
			s.Runtime().APINodes().Watchers().Revision().Prefix(),
			"runtime/api/node/watcher/cached/revision/",
		},
		{
			s.Runtime().APINodes().Watchers().Revision().Node("my-node").Key(),
			"runtime/api/node/watcher/cached/revision/my-node",
		},
		{
			s.Runtime().LastRecordID().Prefix(),
			"runtime/last/record/id/",
		},
		{
			s.Runtime().LastRecordID().InReceiver(exportKey.ReceiverKey).Prefix(),
			"runtime/last/record/id/123/my-receiver/",
		},
		{
			s.Runtime().LastRecordID().ByKey(exportKey).Key(),
			"runtime/last/record/id/123/my-receiver/my-export",
		},
		{
			s.Tasks().Prefix(),
			"task/",
		},
		{
			s.Tasks().InProject(projectID).Prefix(),
			"task/123/",
		},
		{
			s.Tasks().InReceiver(receiverKey).Prefix(),
			"task/123/my-receiver/",
		},
		{
			s.Tasks().InExport(exportKey).Prefix(),
			"task/123/my-receiver/my-export/",
		},
		{
			s.Tasks().ByKey(taskKey).Key(),
			"task/123/my-receiver/some.task/2006-01-02T08:04:05.000Z_abcdef",
		},
		{
			s.SliceStats().InState(slicestate.Writing).Prefix(),
			"stats/slice/active/opened/writing/",
		},
		{
			s.SliceStats().InState(slicestate.Closing).Prefix(),
			"stats/slice/active/opened/writing/", // !
		},
		{
			s.SliceStats().InState(slicestate.Imported).Prefix(),
			"stats/slice/archived/successful/imported/",
		},
		{
			s.SliceStats().InState(slicestate.Imported).InReceiver(receiverKey).Prefix(),
			"stats/slice/archived/successful/imported/123/my-receiver/",
		},
		{
			s.SliceStats().InState(slicestate.Imported).InExport(exportKey).Prefix(),
			"stats/slice/archived/successful/imported/123/my-receiver/my-export/",
		},
		{
			s.SliceStats().InState(slicestate.Imported).InExport(exportKey).ReduceSum().Key(),
			"stats/slice/archived/successful/imported/123/my-receiver/my-export/_reduce_sum",
		},
		{
			s.SliceStats().InState(slicestate.Imported).InFile(fileKey).Prefix(),
			"stats/slice/archived/successful/imported/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.SliceStats().InState(slicestate.Imported).InSlice(sliceKey).Prefix(),
			"stats/slice/archived/successful/imported/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/",
		},
		{
			s.SliceStats().InState(slicestate.Writing).InSlice(sliceKey).NodeID("my-node").Key(),
			"stats/slice/active/opened/writing/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/my-node",
		},
		{
			s.SliceStats().InState(slicestate.Uploaded).InSlice(sliceKey).AllNodesSum().Key(),
			"stats/slice/active/closed/uploaded/123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/_nodes_sum",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func noValidation(_ context.Context, _ any) error {
	return nil
}
