package schema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := schema.New(noValidation)

	time1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	time2 := time1.Add(time.Hour)

	projectID := key.ProjectID(123)
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	mappingKey := key.MappingKey{ExportKey: exportKey, RevisionID: 10}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(time1)}
	sliceKey := key.SliceKey{SliceID: key.SliceID(time2), FileKey: fileKey}
	recordKey := key.RecordKey{SliceKey: sliceKey, ReceivedAt: key.ReceivedAt(time2.Add(time.Hour)), RandomSuffix: "abcdef"}
	taskKey := key.TaskKey{ReceiverKey: receiverKey, Type: "some.task", TaskID: key.FormatTime(time1) + "_abcdef"}

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
			"config/receiver/00000123/",
		},
		{
			s.Configs().Receivers().ByKey(receiverKey).Key(),
			"config/receiver/00000123/my-receiver",
		},
		{
			s.Configs().Exports().Prefix(),
			"config/export/",
		},
		{
			s.Configs().Exports().InReceiver(receiverKey).Prefix(),
			"config/export/00000123/my-receiver/",
		},
		{
			s.Configs().Exports().ByKey(exportKey).Key(),
			"config/export/00000123/my-receiver/my-export",
		},
		{
			s.Configs().Mappings().Prefix(),
			"config/mapping/revision/",
		},
		{
			s.Configs().Mappings().InReceiver(receiverKey).Prefix(),
			"config/mapping/revision/00000123/my-receiver/",
		},
		{
			s.Configs().Mappings().InExport(exportKey).Prefix(),
			"config/mapping/revision/00000123/my-receiver/my-export/",
		},
		{
			s.Configs().Mappings().ByKey(mappingKey).Key(),
			"config/mapping/revision/00000123/my-receiver/my-export/00000010",
		},
		{
			s.Secrets().Tokens().InReceiver(receiverKey).Prefix(),
			"secret/export/token/00000123/my-receiver/",
		},
		{
			s.Secrets().Tokens().InExport(exportKey).Key(),
			"secret/export/token/00000123/my-receiver/my-export",
		},

		{
			s.Files().Prefix(),
			"file/",
		},
		{
			s.Files().InState(filestate.Opened).InReceiver(receiverKey).Prefix(),
			"file/opened/00000123/my-receiver/",
		},
		{
			s.Files().InState(filestate.Opened).InExport(exportKey).Prefix(),
			"file/opened/00000123/my-receiver/my-export/",
		},
		{
			s.Files().Opened().ByKey(fileKey).Key(),
			"file/opened/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z",
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
			"slice/active/opened/writing/00000123/my-receiver/",
		},
		{
			s.Slices().InState(slicestate.Writing).InExport(exportKey).Prefix(),
			"slice/active/opened/writing/00000123/my-receiver/my-export/",
		},
		{
			s.Slices().InState(slicestate.Writing).InFile(fileKey).Prefix(),
			"slice/active/opened/writing/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.Slices().InState(slicestate.Writing).ByKey(sliceKey).Key(),
			"slice/active/opened/writing/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z",
		},
		{
			s.Records().Prefix(),
			"record/",
		},
		{
			s.Records().InReceiver(receiverKey).Prefix(),
			"record/00000123/my-receiver/",
		},
		{
			s.Records().InSlice(sliceKey).Prefix(),
			"record/00000123/my-receiver/my-export/2006-01-02T09:04:05.000Z/",
		},
		{
			s.Records().ByKey(recordKey).Key(),
			"record/00000123/my-receiver/my-export/2006-01-02T09:04:05.000Z/2006-01-02T10:04:05.000Z_abcdef",
		},
		{
			s.Secrets().Tokens().InExport(exportKey).Key(),
			"secret/export/token/00000123/my-receiver/my-export",
		},
		{
			s.Runtime().Prefix(),
			"runtime/",
		},
		{
			s.Runtime().WorkerNodes().Prefix(),
			"runtime/worker/node/",
		},
		{
			s.Runtime().WorkerNodes().Active().Prefix(),
			"runtime/worker/node/active/",
		},
		{
			s.Runtime().WorkerNodes().Active().IDs().Prefix(),
			"runtime/worker/node/active/id/",
		},
		{
			s.Runtime().WorkerNodes().Active().IDs().Node("my-node").Key(),
			"runtime/worker/node/active/id/my-node",
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
			s.Runtime().Lock().Prefix(),
			"runtime/lock/",
		},
		{
			s.Runtime().Lock().Task().Prefix(),
			"runtime/lock/task/",
		},
		{
			s.Runtime().Lock().Task().Prefix(),
			"runtime/lock/task/",
		},
		{
			s.Runtime().Lock().Task().Key("my-lock").Key(),
			"runtime/lock/task/my-lock",
		},
		{
			s.Runtime().LastRecordID().Prefix(),
			"runtime/last/record/id/",
		},
		{
			s.Runtime().LastRecordID().InReceiver(exportKey.ReceiverKey).Prefix(),
			"runtime/last/record/id/00000123/my-receiver/",
		},
		{
			s.Runtime().LastRecordID().ByKey(exportKey).Key(),
			"runtime/last/record/id/00000123/my-receiver/my-export",
		},
		{
			s.Tasks().Prefix(),
			"task/",
		},
		{
			s.Tasks().InProject(projectID).Prefix(),
			"task/00000123/",
		},
		{
			s.Tasks().InReceiver(receiverKey).Prefix(),
			"task/00000123/my-receiver/",
		},
		{
			s.Tasks().ByKey(taskKey).Key(),
			"task/00000123/my-receiver/some.task/2006-01-02T08:04:05.000Z_abcdef",
		},
		{
			s.ReceivedStats().InReceiver(receiverKey).Prefix(),
			"stats/received/00000123/my-receiver/",
		},
		{
			s.ReceivedStats().InExport(exportKey).Prefix(),
			"stats/received/00000123/my-receiver/my-export/",
		},
		{
			s.ReceivedStats().InFile(fileKey).Prefix(),
			"stats/received/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z/",
		},
		{
			s.ReceivedStats().InSlice(sliceKey).Prefix(),
			"stats/received/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/",
		},
		{
			s.ReceivedStats().InSlice(sliceKey).ByNodeID("my-node").Key(),
			"stats/received/00000123/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T09:04:05.000Z/my-node",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func noValidation(_ context.Context, _ any) error {
	return nil
}
