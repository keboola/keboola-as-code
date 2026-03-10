package ignore

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
)

func newTestRegistryWithScheduler(t *testing.T) *registry.Registry {
	t.Helper()
	r := newTestRegistry(t)

	localContent := orderedmap.New()
	localContent.Set("schedule", orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "cronTab", Value: "*/5 * * * *"},
		{Key: "timezone", Value: "UTC"},
	}))

	remoteContent := orderedmap.New()
	remoteContent.Set("schedule", orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "cronTab", Value: "*/10 * * * *"},
		{Key: "timezone", Value: "Europe/Prague"},
	}))

	configKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.scheduler", ID: "456"}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:  configKey,
			Name:       "My Schedule",
			IsDisabled: true,
			Content:    localContent,
		},
		Remote: &model.Config{
			ConfigKey:  configKey,
			Name:       "My Schedule",
			IsDisabled: false,
			Content:    remoteContent,
		},
	}
	require.NoError(t, r.Set(configState))
	return r
}

func findSchedulerConfig(t *testing.T, r *registry.Registry) *model.ConfigState {
	t.Helper()
	for _, c := range r.Configs() {
		if c.ComponentID.String() == "keboola.scheduler" && c.ID.String() == "456" {
			return c
		}
	}
	t.Fatal("scheduler config not found")
	return nil
}

func TestIgnoreFields_IsDisabled_Push(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:isDisabled")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// SyncDirectionPush: copy remote.IsDisabled → local.IsDisabled
	require.NoError(t, file.IgnoreFields(SyncDirectionPush))

	c := findSchedulerConfig(t, r)
	// Local isDisabled should match remote (false), not the original local value (true).
	assert.False(t, c.Local.IsDisabled)
	assert.False(t, c.Remote.IsDisabled)
}

func TestIgnoreFields_IsDisabled_Pull(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:isDisabled")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// SyncDirectionPull: copy local.IsDisabled → remote.IsDisabled
	require.NoError(t, file.IgnoreFields(SyncDirectionPull))

	c := findSchedulerConfig(t, r)
	// Remote isDisabled should match local (true), not the original remote value (false).
	assert.True(t, c.Local.IsDisabled)
	assert.True(t, c.Remote.IsDisabled)
}

func TestIgnoreFields_ContentKey_Push(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:schedule")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// SyncDirectionPush: copy remote schedule → local schedule
	require.NoError(t, file.IgnoreFields(SyncDirectionPush))

	c := findSchedulerConfig(t, r)
	// Local cronTab should now be the remote value.
	localCronTab, found, err := c.Local.Content.GetNested("schedule.cronTab")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "*/10 * * * *", localCronTab)
}

func TestIgnoreFields_ContentKey_Pull(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:schedule")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// SyncDirectionPull: copy local schedule → remote schedule
	require.NoError(t, file.IgnoreFields(SyncDirectionPull))

	c := findSchedulerConfig(t, r)
	// Remote cronTab should now be the local value.
	remoteCronTab, found, err := c.Remote.Content.GetNested("schedule.cronTab")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "*/5 * * * *", remoteCronTab)
}

func TestIgnoreFields_NestedContentKey_Push(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	// Only ignore the nested leaf "schedule.cronTab", not the whole "schedule" object.
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:schedule.cronTab")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// Push: copy remote cronTab → local cronTab; timezone must remain unchanged.
	require.NoError(t, file.IgnoreFields(SyncDirectionPush))

	c := findSchedulerConfig(t, r)
	// Local cronTab should match remote ("*/10 * * * *").
	localCronTab, found, err := c.Local.Content.GetNested("schedule.cronTab")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "*/10 * * * *", localCronTab)
	// Local timezone must be untouched (still "UTC").
	localTZ, found, err := c.Local.Content.GetNested("schedule.timezone")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "UTC", localTZ)
}

func TestIgnoreFields_NoMatchingConfig(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistryWithScheduler(t)
	fs := aferofs.NewMemoryFs()

	// Pattern references a non-existent config ID.
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/999:isDisabled")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())
	require.NoError(t, file.IgnoreFields(SyncDirectionPush))

	// Nothing should change.
	c := findSchedulerConfig(t, r)
	assert.True(t, c.Local.IsDisabled)
	assert.False(t, c.Remote.IsDisabled)
}

func TestIgnoreFields_MissingRemote(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	r := newTestRegistry(t)
	fs := aferofs.NewMemoryFs()

	// Config with only local state (no remote).
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.scheduler", ID: "456"}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:  configKey,
			IsDisabled: true,
			Content:    orderedmap.New(),
		},
	}
	require.NoError(t, r.Set(configState))

	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("kbcignore", "keboola.scheduler/456:isDisabled")))

	file, err := LoadFile(ctx, fs, r, "kbcignore")
	require.NoError(t, err)
	require.NoError(t, file.IgnoreConfigsOrRows())

	// Should not panic, just skip silently.
	require.NoError(t, file.IgnoreFields(SyncDirectionPush))
	assert.True(t, configState.Local.IsDisabled)
}
