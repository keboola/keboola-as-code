package ignore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestFile_IgnoreConfigsOrRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	registry := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "keboola.bar/678/34\nkeboola.foo/345")))

	file, err := LoadFile(ctx, fs, registry, "foo/bar1")
	require.NoError(t, err)

	assert.NoError(t, file.IgnoreConfigsOrRows())

	assert.Len(t, registry.IgnoredConfigRows(), 1)
	assert.Len(t, registry.IgnoredConfigs(), 1)
	assert.Equal(t, registry.IgnoredConfigRows()[0].ID.String(), "34")
	assert.Equal(t, registry.IgnoredConfigs()[0].ID.String(), "345")
}

func Test_applyIgnoredPatterns(t *testing.T) {
	t.Parallel()
	projectState := newTestRegistry(t)

	type args struct {
		pattern string
	}

	file := &File{
		state: projectState,
	}

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "empty patterns",
			args: args{
				pattern: "",
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong pattern",
			args: args{
				pattern: "wrong pattern",
			},
			wantErr: assert.Error,
		},
		{
			name: "too long pattern",
			args: args{
				pattern: "keboola.bar/687/1234/1234",
			},
			wantErr: assert.Error,
		},
		{
			name: "short pattern",
			args: args{
				pattern: "keboola.bar",
			},
			wantErr: assert.Error,
		},
		{
			name: "correct pattern",
			args: args{
				pattern: "keboola.bar/687",
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.wantErr(t, file.applyIgnorePattern(tt.args.pattern), fmt.Sprintf("applyIgnoredPatterns(%v)", tt.args.pattern))
		})
	}
}

func Test_parseIgnoredPatterns(t *testing.T) {
	t.Parallel()

	projectState := newTestRegistry(t)

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
		{
			name: "error",
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
		file := newFile(tt.args.content, projectState)
		got := file.parseIgnoredPatterns()
		assert.Equal(t, tt.want, got)
	}
}
