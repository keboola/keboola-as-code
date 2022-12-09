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
	now, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
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
			s.Configs().Receivers().InProject(123).Prefix(),
			"config/receiver/123/",
		},
		{
			s.Configs().Receivers().ByKey(key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}).Key(),
			"config/receiver/123/my-receiver",
		},
		{
			s.Configs().Exports().Prefix(),
			"config/export/",
		},
		{
			s.Configs().Exports().InReceiver(key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}).Prefix(),
			"config/export/123/my-receiver/",
		},
		{
			s.Configs().Exports().ByKey(key.ExportKey{ExportID: "my-export", ReceiverKey: key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}}).Key(),
			"config/export/123/my-receiver/my-export",
		},
		{
			s.Configs().Mappings().Prefix(),
			"config/mapping/revision/",
		},
		{
			s.Configs().Mappings().InReceiver(key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}).Prefix(),
			"config/mapping/revision/123/my-receiver/",
		},
		{
			s.Configs().Mappings().InExport(key.ExportKey{ExportID: "my-export", ReceiverKey: key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}}).Prefix(),
			"config/mapping/revision/123/my-receiver/my-export/",
		},
		{
			s.Configs().Mappings().ByKey(key.MappingKey{ExportKey: key.ExportKey{ExportID: "my-export", ReceiverKey: key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}}, RevisionID: 10}).Key(),
			"config/mapping/revision/123/my-receiver/my-export/00000010",
		},
		{
			s.Secrets().Tokens().InReceiver(key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}).Prefix(),
			"secret/export/token/123/my-receiver/",
		},
		{
			s.Secrets().Tokens().InExport(key.ExportKey{ExportID: "my-export", ReceiverKey: key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}}).Key(),
			"secret/export/token/123/my-receiver/my-export",
		},
		{
			s.Records().Prefix(),
			"record/",
		},
		{
			s.Records().ByKey(key.RecordKey{
				ReceivedAt:   now.Add(time.Hour),
				RandomSuffix: "abcdef",
				ExportKey: key.ExportKey{
					ExportID: "my-export",
					ReceiverKey: key.ReceiverKey{
						ProjectID:  123,
						ReceiverID: "my-receiver",
					},
				},
				SliceID: now,
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
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func noValidation(ctx context.Context, value any) error {
	return nil
}
