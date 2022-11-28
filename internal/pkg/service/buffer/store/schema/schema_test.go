package schema_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := schema.New(noValidation)
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
			s.Configs().Receivers().InProject(123).ID("my-receiver").Key(),
			"config/receiver/123/my-receiver",
		},
		{
			s.Configs().Exports().Prefix(),
			"config/export/",
		},
		{
			s.Configs().Exports().InProject(123).Prefix(),
			"config/export/123/",
		},
		{
			s.Configs().Exports().InProject(123).InReceiver("my-receiver").Prefix(),
			"config/export/123/my-receiver/",
		},
		{
			s.Configs().Exports().InProject(123).InReceiver("my-receiver").ID("my-export").Key(),
			"config/export/123/my-receiver/my-export",
		},
		{
			s.Configs().Mappings().Prefix(),
			"config/mapping/revision/",
		},
		{
			s.Configs().Mappings().InProject(123).Prefix(),
			"config/mapping/revision/123/",
		},
		{
			s.Configs().Mappings().InProject(123).InReceiver("my-receiver").Prefix(),
			"config/mapping/revision/123/my-receiver/",
		},
		{
			s.Configs().Mappings().InProject(123).InReceiver("my-receiver").InExport("my-export").Prefix(),
			"config/mapping/revision/123/my-receiver/my-export/",
		},
		{
			s.Configs().Mappings().InProject(123).InReceiver("my-receiver").InExport("my-export").Revision(100).Key(),
			"config/mapping/revision/123/my-receiver/my-export/00000100",
		},
		{
			s.Records().Prefix(),
			"record/",
		},
		{
			s.Records().InProject(123).Prefix(),
			"record/123/",
		},
		{
			s.Records().InProject(123).InReceiver("my-receiver").Prefix(),
			"record/123/my-receiver/",
		},
		{
			s.Records().InProject(123).InReceiver("my-receiver").InExport("my-export").Prefix(),
			"record/123/my-receiver/my-export/",
		},
		{
			s.Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").Prefix(),
			"record/123/my-receiver/my-export/fileID/",
		},
		{
			s.Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").InSlice("sliceID").Prefix(),
			"record/123/my-receiver/my-export/fileID/sliceID/",
		},
		{
			s.Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").InSlice("sliceID").ID("2022-...").Key(),
			"record/123/my-receiver/my-export/fileID/sliceID/2022-...",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func noValidation(ctx context.Context, value any) error {
	return nil
}
