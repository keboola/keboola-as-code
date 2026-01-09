package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultDocFields(t *testing.T) {
	t.Parallel()

	fields := DefaultDocFields()
	assert.NotNil(t, fields)
	assert.Equal(t, "Every sync", fields.UpdateFrequency)
}

func TestBucketsIndexDocFields(t *testing.T) {
	t.Parallel()

	fields := BucketsIndexDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Comment, "buckets")
	assert.Contains(t, fields.Purpose, "Catalog")
	assert.Equal(t, "Every sync", fields.UpdateFrequency)
}

func TestTableMetadataDocFields(t *testing.T) {
	t.Parallel()

	fields := TableMetadataDocFields("shopify")
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Comment, "tables")
	assert.Contains(t, fields.Purpose, "shopify")
}

func TestTransformationsIndexDocFields(t *testing.T) {
	t.Parallel()

	fields := TransformationsIndexDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Comment, "transformation")
	assert.Contains(t, fields.Purpose, "Catalog")
}

func TestJobsIndexDocFields(t *testing.T) {
	t.Parallel()

	fields := JobsIndexDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Comment, "jobs")
	assert.Contains(t, fields.Purpose, "Job execution")
}

func TestManifestExtendedDocFields(t *testing.T) {
	t.Parallel()

	fields := ManifestExtendedDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Purpose, "project overview")
}

func TestSourcesIndexDocFields(t *testing.T) {
	t.Parallel()

	fields := SourcesIndexDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Purpose, "data sources")
}

func TestComponentsIndexDocFields(t *testing.T) {
	t.Parallel()

	fields := ComponentsIndexDocFields()
	assert.NotNil(t, fields)
	assert.Contains(t, fields.Comment, "component")
}
