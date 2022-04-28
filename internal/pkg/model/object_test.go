package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBranchMetadata_AddTemplateUsage(t *testing.T) {
	t.Parallel()

	b := BranchMetadata{}
	err := b.AddTemplateUsage("inst1", "tmpl1", "1.0.0")
	assert.NoError(t, err)
	assert.Len(t, b, 1)
	meta, found := b["KBC.KAC.templates.instances"]
	assert.True(t, found)
	assert.Equal(t, `[{"instanceId":"inst1","templateId":"tmpl1","version":"1.0.0"}]`, meta)

	usages, err := b.TemplatesUsages()
	assert.NoError(t, err)
	assert.Equal(t, TemplateUsageRecords{
		{
			InstanceId: "inst1",
			TemplateId: "tmpl1",
			Version:    "1.0.0",
		},
	}, usages)

	err = b.AddTemplateUsage("inst2", "tmpl2", "2.0.0")
	assert.NoError(t, err)

	usages, err = b.TemplatesUsages()
	assert.NoError(t, err)
	assert.Equal(t, TemplateUsageRecords{
		{
			InstanceId: "inst1",
			TemplateId: "tmpl1",
			Version:    "1.0.0",
		},
		{
			InstanceId: "inst2",
			TemplateId: "tmpl2",
			Version:    "2.0.0",
		},
	}, usages)
}
