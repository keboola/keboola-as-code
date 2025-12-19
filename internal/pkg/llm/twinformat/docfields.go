package twinformat

// DocFieldsConfig contains the documentation fields for JSON output files.
// Every JSON file in the twin format output must have these fields.
type DocFieldsConfig struct {
	Comment         string
	Purpose         string
	UpdateFrequency string
	Security        string
	Retention       string
}

// DefaultDocFields returns the default documentation fields configuration.
func DefaultDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		UpdateFrequency: "Every sync",
	}
}

// BucketsIndexDocFields returns documentation fields for buckets/index.json.
func BucketsIndexDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: GET /v2/storage/buckets - aggregate all buckets",
		Purpose:         "Catalog of all buckets for fast lookup without scanning directories",
		UpdateFrequency: "Every sync",
	}
}

// TableMetadataDocFields returns documentation fields for table metadata.json files.
func TableMetadataDocFields(source string) *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: GET /v2/storage/tables/{table_id}?include=columns,metadata + computed dependencies",
		Purpose:         "Table metadata for " + source,
		UpdateFrequency: "On table structure changes",
	}
}

// TransformationsIndexDocFields returns documentation fields for transformations/index.json.
func TransformationsIndexDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: Scan transformation configs + group by platform",
		Purpose:         "Catalog of all transformations grouped by platform",
		UpdateFrequency: "Every sync",
	}
}

// TransformationMetadataDocFields returns documentation fields for transformation metadata.json files.
func TransformationMetadataDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: From transformation config + platform detection + computed dependencies + job queue status",
		Purpose:         "Complete transformation configuration and data flow dependencies",
		UpdateFrequency: "On transformation config changes and job completion",
	}
}

// JobsIndexDocFields returns documentation fields for jobs/index.json.
func JobsIndexDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: GET /search/jobs + aggregate statistics",
		Purpose:         "Job execution statistics and summary",
		UpdateFrequency: "Every hour or on job completion",
	}
}

// JobMetadataDocFields returns documentation fields for individual job files.
func JobMetadataDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: GET /search/jobs/{job_id}",
		Purpose:         "Individual job execution details",
		UpdateFrequency: "On job completion",
	}
}

// ManifestExtendedDocFields returns documentation fields for manifest-extended.json.
func ManifestExtendedDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: GET /v2/storage/tokens/verify + computed statistics",
		Purpose:         "Complete project overview in one file for fast AI analysis",
		UpdateFrequency: "Every sync",
	}
}

// SourcesIndexDocFields returns documentation fields for indices/sources.json.
func SourcesIndexDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: Inferred from bucket names + GET /v2/storage components",
		Purpose:         "Registry of data sources with bucket and table counts",
		UpdateFrequency: "Every sync",
	}
}

// ComponentsIndexDocFields returns documentation fields for components/index.json.
func ComponentsIndexDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: Scan component configs + group by type",
		Purpose:         "Catalog of all components grouped by type",
		UpdateFrequency: "Every sync",
	}
}

// ComponentMetadataDocFields returns documentation fields for component metadata.json files.
func ComponentMetadataDocFields() *DocFieldsConfig {
	return &DocFieldsConfig{
		Comment:         "GENERATION: From component config + job queue status",
		Purpose:         "Complete component configuration and execution status",
		UpdateFrequency: "On component config changes and job completion",
	}
}
