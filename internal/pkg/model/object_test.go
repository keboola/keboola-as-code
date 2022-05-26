package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestBranchMetadata_UpsertTemplateInstance_New(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second).UTC()
	b := BranchMetadata{}

	// First instance
	assert.NoError(t, b.UpsertTemplateInstance(now, "inst1", "Instance 1", "tmpl1", "repo", "1.0.0", "12345", &TemplateMainConfig{ConfigId: "1234", ComponentId: "foo.bar"}))
	assert.Len(t, b, 1)

	meta, found := b["KBC.KAC.templates.instances"]
	assert.True(t, found)

	testhelper.AssertWildcards(t, `[{"instanceId":"inst1","instanceName":"Instance 1","templateId":"tmpl1","repositoryName":"repo","version":"1.0.0","created":{"date":"%s","tokenId":"12345"},"updated":{"date":"%s","tokenId":"12345"},"mainConfig":{"configId":"1234","componentId":"foo.bar"}}]`, meta, "case 1")

	usages, err := b.TemplatesInstances()
	assert.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceId:     "inst1",
			InstanceName:   "Instance 1",
			TemplateId:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.0.0",
			Created:        ChangedByRecord{Date: now, TokenId: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenId: "12345"},
			MainConfig:     &TemplateMainConfig{ConfigId: "1234", ComponentId: "foo.bar"},
		},
	}, usages)

	// Second instance
	assert.NoError(t, b.UpsertTemplateInstance(now, "inst2", "Instance 2", "tmpl2", "repo", "2.0.0", "789", nil))
	usages, err = b.TemplatesInstances()
	assert.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceId:     "inst1",
			InstanceName:   "Instance 1",
			TemplateId:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.0.0",
			Created:        ChangedByRecord{Date: now, TokenId: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenId: "12345"},
			MainConfig:     &TemplateMainConfig{ConfigId: "1234", ComponentId: "foo.bar"},
		},
		{
			InstanceId:     "inst2",
			InstanceName:   "Instance 2",
			TemplateId:     "tmpl2",
			RepositoryName: "repo",
			Version:        "2.0.0",
			Created:        ChangedByRecord{Date: now, TokenId: "789"},
			Updated:        ChangedByRecord{Date: now, TokenId: "789"},
		},
	}, usages)

	// First instance - update
	assert.NoError(t, b.UpsertTemplateInstance(now, "inst1", "Modified Instance 1", "tmpl1", "repo", "1.2.3", "789", &TemplateMainConfig{ConfigId: "7890", ComponentId: "foo.bar"}))
	usages, err = b.TemplatesInstances()
	assert.NoError(t, err)
	assert.Equal(t, TemplatesInstances{
		{
			InstanceId:     "inst1",
			InstanceName:   "Modified Instance 1",
			TemplateId:     "tmpl1",
			RepositoryName: "repo",
			Version:        "1.2.3",
			Created:        ChangedByRecord{Date: now, TokenId: "12345"},
			Updated:        ChangedByRecord{Date: now, TokenId: "789"},
			MainConfig:     &TemplateMainConfig{ConfigId: "7890", ComponentId: "foo.bar"},
		},
		{
			InstanceId:     "inst2",
			InstanceName:   "Instance 2",
			TemplateId:     "tmpl2",
			RepositoryName: "repo",
			Version:        "2.0.0",
			Created:        ChangedByRecord{Date: now, TokenId: "789"},
			Updated:        ChangedByRecord{Date: now, TokenId: "789"},
		},
	}, usages)
}

func TestBranchMetadata_DeleteTemplateUsage(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second).UTC()
	usage1 := TemplateInstance{
		InstanceId:     "inst1",
		InstanceName:   "Instance 1",
		TemplateId:     "tmpl1",
		RepositoryName: "repo",
		Version:        "1.0.0",
		Created:        ChangedByRecord{Date: now, TokenId: "12345"},
		Updated:        ChangedByRecord{Date: now, TokenId: "12345"},
		MainConfig:     &TemplateMainConfig{ConfigId: "1234", ComponentId: "foo.bar"},
	}
	usage2 := TemplateInstance{
		InstanceId:     "inst2",
		InstanceName:   "Instance 2",
		TemplateId:     "tmpl1",
		RepositoryName: "repo",
		Version:        "1.0.0",
		Created:        ChangedByRecord{Date: now, TokenId: "12345"},
		Updated:        ChangedByRecord{Date: now, TokenId: "12345"},
		MainConfig:     &TemplateMainConfig{ConfigId: "1234", ComponentId: "foo.bar"},
	}
	encUsages, err := json.EncodeString(TemplatesInstances{usage1, usage2}, false)
	assert.NoError(t, err)

	b := BranchMetadata{}
	b["KBC.KAC.templates.instances"] = encUsages

	usage, found, err := b.TemplateInstance("inst1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, &usage1, usage)

	err = b.DeleteTemplateUsage("inst1")
	assert.NoError(t, err)

	usages, err := b.TemplatesInstances()
	assert.NoError(t, err)
	assert.Len(t, usages, 1)

	_, found, err = b.TemplateInstance("inst1")
	assert.NoError(t, err)
	assert.False(t, found)
}
