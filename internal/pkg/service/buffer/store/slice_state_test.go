package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDeps "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	storePkg "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_SetSliceState_Transitions(t *testing.T) {
	t.Parallel()

	// Test all transitions
	testCases := []struct{ from, to slicestate.State }{
		{slicestate.Writing, slicestate.Closing},
		{slicestate.Closing, slicestate.Uploading},
		{slicestate.Uploading, slicestate.Failed},
		{slicestate.Failed, slicestate.Uploading},
		{slicestate.Uploading, slicestate.Uploaded},
		{slicestate.Uploaded, slicestate.Imported},
	}

	ctx := context.Background()
	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01+07:00")
	clk := clock.NewMock()
	clk.Set(now)
	d, mock := bufferDeps.NewMockedServiceScope(t, config.NewServiceConfig(), dependencies.WithClock(clk), dependencies.WithEnabledEtcdClient())
	client := mock.TestEtcdClient()
	store := storePkg.New(d)
	slice := sliceForTest()

	// Create slice
	assert.NoError(t, store.CreateSlice(ctx, slice))

	for _, tc := range testCases {
		// Trigger transition
		desc := fmt.Sprintf("%s -> %s", tc.from, tc.to)
		err := store.SetSliceState(ctx, &slice, tc.to)
		assert.NoError(t, err, desc)
		assert.Equal(t, tc.to, slice.State, desc)
		expected := `
<<<<<
slice/<FULL_STATE>/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
%A
  "state": "<FULL_STATE>",%A
  "<SHORT_STATE>At": "2009-12-31T18:01:01.000Z"%A
>>>>>
`
		expected = strings.ReplaceAll(expected, "<FULL_STATE>", tc.to.String())
		expected = strings.ReplaceAll(expected, "<SHORT_STATE>", tc.to.StateShort())
		etcdhelper.AssertKVsString(t, client, expected)

		// Test duplicated transition -> nop
		slice.State = tc.from
		err = store.SetSliceState(ctx, &slice, tc.to)
		assert.Error(t, err, desc)
		wildcards.Assert(t, `slice "%s" is already in the "%s" state`, err.Error(), desc)
		assert.Equal(t, tc.to, slice.State, desc)
	}
}

func sliceForTest() model.Slice {
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: key.FileID(time1.UTC()), ExportKey: exportKey}
	mapping := model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: 1,
		},
		TableID: keboola.TableID{
			BucketID: keboola.BucketID{
				Stage:      keboola.BucketStageIn,
				BucketName: "c-bucket",
			},
			TableName: "table",
		},
		Incremental: false,
		Columns: []column.Column{
			column.Body{Name: "body"},
		},
	}
	return model.NewSlice(fileKey, time2, mapping, 1, &keboola.FileUploadCredentials{})
}
