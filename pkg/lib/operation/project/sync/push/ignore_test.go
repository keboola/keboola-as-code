package push

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func Test_parseIgnoredPatterns(t *testing.T) {
	t.Parallel()
	type args struct {
		content string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "with comments",
			args: args{
				content: "##dataapps\nkeboola.data-apps/12345\n##dataapps2\nkeboola.data-apps/123/rowId123\n##dataaps3\nkeboola.data-apps/12",
			},
			want: []string{
				"keboola.data-apps/12345",
				"keboola.data-apps/123/rowId123",
				"keboola.data-apps/12",
			},
		},
		{
			name: "with empty lines",
			args: args{
				content: "##dataapps\n\n\n\n\n\nkeboola.data-apps/12345\n\t##dataapps2\nkeboola.data-apps/123\n##dataaps3\nkeboola.data-apps/12",
			},
			want: []string{
				"keboola.data-apps/12345",
				"keboola.data-apps/123",
				"keboola.data-apps/12",
			},
		},
		{
			name: "empty file",
			args: args{
				content: "\n",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := parseIgnoredPatterns(tt.args.content); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseIgnoredPatterns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_setIgnoredConfigsOrRows(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	ctx := context.Background()
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar`, "keboola.bar/678/34\nkeboola.foo/345")))

	projectState := state.NewRegistry(knownpaths.NewNop(ctx), naming.NewRegistry(), model.NewComponentsMap(nil), model.SortByPath)

	// Branch 1
	branch1Key := model.BranchKey{ID: 123}
	branch1 := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branch1Key,
		},
		Local: &model.Branch{
			Name:      "Main",
			IsDefault: true,
		},
	}
	assert.NoError(t, projectState.Set(branch1))

	// Branch 2
	branch2Key := model.BranchKey{ID: 567}
	branch2 := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branch2Key,
		},
		Local: &model.Branch{
			Name:      "Foo Bar Branch",
			IsDefault: false,
		},
	}
	assert.NoError(t, projectState.Set(branch2))

	// Config 1
	config1Key := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo", ID: `345`}
	config1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: config1Key},
		Local: &model.Config{
			Name: "Config 1",
		},
	}
	assert.NoError(t, projectState.Set(config1))

	// Config 2
	config2Key := model.ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `678`}
	config2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: config2Key},
		Local: &model.Config{
			Name: "Config 2",
		},
	}
	assert.NoError(t, projectState.Set(config2))

	// Config Row 1
	row1Key := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `12`}
	row1 := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: row1Key},
		Local: &model.ConfigRow{
			Name: "Config Row 1",
		},
	}
	assert.NoError(t, projectState.Set(row1))

	// Config Row 2
	row2Key := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `34`}
	row2 := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: row2Key},
		Local: &model.ConfigRow{
			Name: "Config Row 2",
		},
	}

	assert.NoError(t, projectState.Set(row2))

	assert.NoError(t, setIgnoredConfigsOrRows(ctx, projectState, fs, "foo/bar"))

	assert.Len(t, projectState.IgnoredConfigRows(), 1)
	assert.Len(t, projectState.IgnoredConfigs(), 1)
}
