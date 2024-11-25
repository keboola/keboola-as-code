package model

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

func TestBranchMetadata_UpsertTemplateInstance_New(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second).UTC()
	b := BranchMetadata{}

	// First instance
	require.NoError(t, b.UpsertTemplateInstance(now, "inst1", "Instance 1", "tmpl1", "repo", "1.0.0", "12345", &TemplateMainConfig{ConfigID: "1234", ComponentID: "foo.bar"}))
	assert.Len(t, b, 1)

	meta, found := b["KBC.KAC.templates.instances"]
	assert.True(t, found)

	wildcards.Assert(t, `[{"instanceId":"inst1","instanceName":"Instance 1","templateId":"tmpl1","repositoryName":"repo","version":"1.0.0","created":{"date":"%s","tokenId":"12345"},"updated":{"date":"%s","tokenId":"12345"},"mainConfig":{"configId":"1234","componentId":"foo.bar"}}]`, meta, "case 1")

	usages, err := b.TemplatesInstances()
	require.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceID:     "inst1",
			InstanceName:   "Instance 1",
			TemplateID:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.0.0",
			Created:        ChangedByRecord{Date: now, TokenID: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenID: "12345"},
			MainConfig:     &TemplateMainConfig{ConfigID: "1234", ComponentID: "foo.bar"},
		},
	}, usages)

	// Second instance
	require.NoError(t, b.UpsertTemplateInstance(now, "inst2", "Instance 2", "tmpl2", "repo", "2.0.0", "789", nil))
	usages, err = b.TemplatesInstances()
	require.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceID:     "inst1",
			InstanceName:   "Instance 1",
			TemplateID:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.0.0",
			Created:        ChangedByRecord{Date: now, TokenID: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenID: "12345"},
			MainConfig:     &TemplateMainConfig{ConfigID: "1234", ComponentID: "foo.bar"},
		},
		{
			InstanceID:     "inst2",
			InstanceName:   "Instance 2",
			TemplateID:     "tmpl2",
			RepositoryName: "repo",
			Version:        "2.0.0",
			Created:        ChangedByRecord{Date: now, TokenID: "789"},
			Updated:        ChangedByRecord{Date: now, TokenID: "789"},
		},
	}, usages)

	// First instance - update
	require.NoError(t, b.UpsertTemplateInstance(now, "inst1", "Modified Instance 1", "tmpl1", "repo", "1.2.3", "789", &TemplateMainConfig{ConfigID: "7890", ComponentID: "foo.bar"}))
	usages, err = b.TemplatesInstances()
	require.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceID:     "inst2",
			InstanceName:   "Instance 2",
			TemplateID:     "tmpl2",
			RepositoryName: "repo",
			Version:        "2.0.0",
			Created:        ChangedByRecord{Date: now, TokenID: "789"},
			Updated:        ChangedByRecord{Date: now, TokenID: "789"},
		},
		{
			InstanceID:     "inst1",
			InstanceName:   "Modified Instance 1",
			TemplateID:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.2.3",
			Created:        ChangedByRecord{Date: now, TokenID: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenID: "789"},
			MainConfig:     &TemplateMainConfig{ConfigID: "7890", ComponentID: "foo.bar"},
		},
	}, usages)
}

func TestBranchMetadata_DeleteTemplateUsage(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second).UTC()
	usage1 := TemplateInstance{
		InstanceID:     "inst1",
		InstanceName:   "Instance 1",
		TemplateID:     "tmpl1",
		RepositoryName: "repo",
		Version:        "1.0.0",
		Created:        ChangedByRecord{Date: now, TokenID: "12345"},
		Updated:        ChangedByRecord{Date: now, TokenID: "12345"},
		MainConfig:     &TemplateMainConfig{ConfigID: "1234", ComponentID: "foo.bar"},
	}
	usage2 := TemplateInstance{
		InstanceID:     "inst2",
		InstanceName:   "Instance 2",
		TemplateID:     "tmpl1",
		RepositoryName: "repo",
		Version:        "1.0.0",
		Created:        ChangedByRecord{Date: now, TokenID: "12345"},
		Updated:        ChangedByRecord{Date: now, TokenID: "12345"},
		MainConfig:     &TemplateMainConfig{ConfigID: "1234", ComponentID: "foo.bar"},
	}
	encUsages, err := json.EncodeString(TemplatesInstances{usage1, usage2}, false)
	require.NoError(t, err)

	b := BranchMetadata{}
	b["KBC.KAC.templates.instances"] = encUsages

	usage, found, err := b.TemplateInstance("inst1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, &usage1, usage)

	err = b.DeleteTemplateUsage("inst1")
	require.NoError(t, err)

	usages, err := b.TemplatesInstances()
	require.NoError(t, err)
	assert.Len(t, usages, 1)

	_, found, err = b.TemplateInstance("inst1")
	require.NoError(t, err)
	assert.False(t, found)
}
