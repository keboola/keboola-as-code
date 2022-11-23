package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	cases := []keyTestCase{
		{
			Configs().Prefix(),
			"config/",
		},
		{
			Configs().Receivers().Prefix(),
			"config/receiver/",
		},
		{
			Configs().Receivers().InProject(123).Prefix(),
			"config/receiver/123/",
		},
		{
			Configs().Receivers().InProject(123).ID("my-receiver").Key(),
			"config/receiver/123/my-receiver",
		},
		{
			Configs().Exports().Prefix(),
			"config/export/",
		},
		{
			Configs().Exports().InProject(123).Prefix(),
			"config/export/123/",
		},
		{
			Configs().Exports().InProject(123).InReceiver("my-receiver").Prefix(),
			"config/export/123/my-receiver/",
		},
		{
			Configs().Exports().InProject(123).InReceiver("my-receiver").ID("my-export").Key(),
			"config/export/123/my-receiver/my-export",
		},
		{
			Configs().Mappings().Prefix(),
			"config/mapping/revision/",
		},
		{
			Configs().Mappings().InProject(123).Prefix(),
			"config/mapping/revision/123/",
		},
		{
			Configs().Mappings().InProject(123).InReceiver("my-receiver").Prefix(),
			"config/mapping/revision/123/my-receiver/",
		},
		{
			Configs().Mappings().InProject(123).InReceiver("my-receiver").InExport("my-export").Prefix(),
			"config/mapping/revision/123/my-receiver/my-export/",
		},
		{
			Configs().Mappings().InProject(123).InReceiver("my-receiver").InExport("my-export").Revision(100).Key(),
			"config/mapping/revision/123/my-receiver/my-export/00000100",
		},
		{
			Records().Prefix(),
			"record/",
		},
		{
			Records().InProject(123).Prefix(),
			"record/123/",
		},
		{
			Records().InProject(123).InReceiver("my-receiver").Prefix(),
			"record/123/my-receiver/",
		},
		{
			Records().InProject(123).InReceiver("my-receiver").InExport("my-export").Prefix(),
			"record/123/my-receiver/my-export/",
		},
		{
			Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").Prefix(),
			"record/123/my-receiver/my-export/fileID/",
		},
		{
			Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").InSlice("sliceID").Prefix(),
			"record/123/my-receiver/my-export/fileID/sliceID/",
		},
		{
			Records().InProject(123).InReceiver("my-receiver").InExport("my-export").InFile("fileID").InSlice("sliceID").ID("2022-...").Key(),
			"record/123/my-receiver/my-export/fileID/sliceID/2022-...",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}
