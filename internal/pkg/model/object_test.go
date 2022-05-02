package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestBranchMetadata_AddTemplateUsage(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second).UTC()

	b := BranchMetadata{}
	err := b.AddTemplateUsage("inst1", "Instance 1", "tmpl1", "repo", "1.0.0", "12345", &ConfigKey{Id: "1234", ComponentId: "foo.bar"})
	assert.NoError(t, err)
	assert.Len(t, b, 1)
	meta, found := b["KBC.KAC.templates.instances"]
	assert.True(t, found)
	testhelper.AssertWildcards(t, `[{"instanceId":"inst1","instanceName":"Instance 1","templateId":"tmpl1","repositoryName":"repo","version":"1.0.0","created":{"date":"%s","tokenId":"12345"},"updated":{"date":"%s","tokenId":"12345"},"mainConfig":{"configId":"1234","componentId":"foo.bar"}}]`, meta, "case 1")

	usages, err := b.TemplatesUsages()
	assert.NoError(t, err)
	assert.Equal(t, TemplateUsageRecords{
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

	err = b.AddTemplateUsage("inst2", "Instance 2", "tmpl2", "repo", "2.0.0", "789", nil)
	assert.NoError(t, err)

	usages, err = b.TemplatesUsages()
	assert.NoError(t, err)
	assert.Equal(t, TemplateUsageRecords{
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
}
