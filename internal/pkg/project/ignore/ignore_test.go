package ignore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestFile_IgnoreObjects(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	registry := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "keboola.bar/678/34\nkeboola.foo/345")))

	file, err := LoadFile(ctx, fs, registry, "foo/bar1")
	require.NoError(t, err)

	require.NoError(t, file.IgnoreObjects())

	assert.Len(t, registry.IgnoredConfigRows(), 1)
	assert.Len(t, registry.IgnoredConfigs(), 1)
	assert.Equal(t, "34", registry.IgnoredConfigRows()[0].ID.String())
	assert.Equal(t, "345", registry.IgnoredConfigs()[0].ID.String())
}

func TestFile_IgnoreObjects_AllNotificationsForConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "keboola.foo/345/notifications")))

	file, err := LoadFile(ctx, fs, r, "foo/bar1")
	require.NoError(t, err)

	require.NoError(t, file.IgnoreObjects())

	ignored := r.IgnoredNotifications()
	require.Len(t, ignored, 1)
	assert.Equal(t, "sub-001", string(ignored[0].ID))
}

func TestFile_IgnoreObjects_SpecificNotification(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "keboola.foo/345/notifications/sub-001")))

	file, err := LoadFile(ctx, fs, r, "foo/bar1")
	require.NoError(t, err)

	require.NoError(t, file.IgnoreObjects())

	ignored := r.IgnoredNotifications()
	require.Len(t, ignored, 1)
	assert.Equal(t, "sub-001", string(ignored[0].ID))
}

func TestFile_IgnoreObjects_Branch(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "branch/Main")))

	file, err := LoadFile(ctx, fs, r, "foo/bar1")
	require.NoError(t, err)

	require.NoError(t, file.IgnoreObjects())

	ignored := r.IgnoredBranches()
	require.Len(t, ignored, 1)
	assert.Equal(t, "123", ignored[0].ID.String())
}

func TestFile_IgnoreObjects_BranchWithSlashInName(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithSlashBranch(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "branch/feature/foo")))

	file, err := LoadFile(ctx, fs, r, "foo/bar1")
	require.NoError(t, err)

	require.NoError(t, file.IgnoreObjects())

	// Branch "feature/foo" should be ignored, not misidentified as a config-row pattern.
	ignored := r.IgnoredBranches()
	require.Len(t, ignored, 1)
	assert.Equal(t, "789", ignored[0].ID.String())
	assert.Empty(t, r.IgnoredConfigRows())
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
			name: "notifications all pattern",
			args: args{
				pattern: "keboola.foo/345/notifications",
			},
			wantErr: assert.NoError,
		},
		{
			name: "notifications specific pattern",
			args: args{
				pattern: "keboola.foo/345/notifications/sub-001",
			},
			wantErr: assert.NoError,
		},
		{
			name: "notifications too long pattern",
			args: args{
				pattern: "keboola.foo/345/notifications/sub-001/extra",
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
		{
			name: "branch pattern",
			args: args{
				pattern: "branch/Main",
			},
			wantErr: assert.NoError,
		},
		{
			name: "branch pattern with slash in name",
			args: args{
				pattern: "branch/feature/foo",
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
