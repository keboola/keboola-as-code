package model

import (
	"encoding/json"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestComponentDefaultBucket(t *testing.T) {
	t.Parallel()

	componentJSON := `
{
  "id": "keboola.ex-aws-s3",
  "type": "extractor",
  "name": "AWS S3",
  "description": "Object storage built to store and retrieve any amount of data from anywhere.",
  "longDescription": "This component loads a single or multiple CSV files from a single or multiple AWS S3 buckets",
  "version": 83,
  "complexity": "medium",
  "category": "files",
  "hasUI": false,
  "hasRun": false,
  "ico32": "https://ui.keboola-assets.com/developer-portal/icons/keboola.ex-aws-s3/32/1576250595503.png",
  "ico64": "https://ui.keboola-assets.com/developer-portal/icons/keboola.ex-aws-s3/64/1576250595503.png",
  "data": {
    "definition": {
      "type": "aws-ecr",
      "uri": "147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.ex-aws-s3",
      "tag": "v3.14.1",
      "digest": "sha256:025a3c4d5b3794fbbbb600ea501215fa0d44055a0445687d1972b90609bc2bac",
      "repository": { "region": "us-east-1" }
    },
    "vendor": { "contact": ["Keboola :(){:|:&};: s.r.o."] },
    "configuration_format": "json",
    "network": "bridge",
    "memory": "6144m",
    "process_timeout": 21600,
    "forward_token": false,
    "forward_token_details": false,
    "default_bucket": true,
    "default_bucket_stage": "in",
    "staging_storage": { "input": "local", "output": "local" },
    "synchronous_actions": ["getExternalId"],
    "image_parameters": {}
  },
  "flags": ["genericDockerUI", "genericDockerUI-processors", "encrypt"],
  "configurationSchema": {},
  "configurationRowSchema": {},
  "emptyConfiguration": {},
  "emptyConfigurationRow": {},
  "uiOptions": {},
  "configurationDescription": null,
  "features": ["has-sample-data", "has-simplified-ui"],
  "expiredOn": null,
  "uri": "https://syrup.north-europe.azure.keboola.com/docker/keboola.ex-aws-s3",
  "documentationUrl": "https://help.keboola.com/extractors/other/aws-s3/"
}`
	component := keboola.Component{}
	_ = json.Unmarshal([]byte(componentJSON), &component)
	assert.Equal(t, keboola.ComponentID("keboola.ex-aws-s3"), component.ID)
	assert.Equal(t, "AWS S3", component.Name)
	assert.IsType(t, keboola.ComponentData{}, component.Data)
	assert.True(t, component.Data.DefaultBucket)
	assert.Equal(t, "in", component.Data.DefaultBucketStage)

	m := NewComponentsMap([]*keboola.Component{&component})

	expected1 := map[keboola.ComponentID]string{"keboola.ex-aws-s3": "in.c-keboola-ex-aws-s3-"}
	assert.Equal(t, expected1, m.defaultBucketsByComponentID)
	expected2 := map[string]keboola.ComponentID{"in.c-keboola-ex-aws-s3-": "keboola.ex-aws-s3"}
	assert.Equal(t, expected2, m.defaultBucketsByPrefix)
}

func TestMatchDefaultBucketInTableId(t *testing.T) {
	t.Parallel()

	m := NewComponentsMap(nil)
	m.defaultBucketsByComponentID = map[keboola.ComponentID]string{"keboola.ex-aws-s3": "in.c-keboola-ex-aws-s3-"}
	m.defaultBucketsByPrefix = map[string]keboola.ComponentID{"in.c-keboola-ex-aws-s3-": "keboola.ex-aws-s3"}

	componentID, configID, matchesDefaultBucket := m.GetDefaultBucketByTableID("in.c-crm.orders")
	assert.Equal(t, keboola.ComponentID(""), componentID)
	assert.Equal(t, keboola.ConfigID(""), configID)
	assert.False(t, matchesDefaultBucket)

	componentID, configID, matchesDefaultBucket = m.GetDefaultBucketByTableID("in.c-keboola-ex-aws-s3-123456.orders")
	assert.Equal(t, keboola.ComponentID("keboola.ex-aws-s3"), componentID)
	assert.Equal(t, keboola.ConfigID("123456"), configID)
	assert.True(t, matchesDefaultBucket)

	componentID, configID, matchesDefaultBucket = m.GetDefaultBucketByTableID("in.c-keboola-ex-aws-s3-123456.my-orders")
	assert.Equal(t, keboola.ComponentID("keboola.ex-aws-s3"), componentID)
	assert.Equal(t, keboola.ConfigID("123456"), configID)
	assert.True(t, matchesDefaultBucket)

	componentID, configID, matchesDefaultBucket = m.GetDefaultBucketByTableID("in.c-keboola-ex-aws-s3.orders")
	assert.Equal(t, keboola.ComponentID(""), componentID)
	assert.Equal(t, keboola.ConfigID(""), configID)
	assert.False(t, matchesDefaultBucket)
}

func TestGetDefaultBucket(t *testing.T) {
	t.Parallel()

	m := NewComponentsMap(nil)
	m.defaultBucketsByComponentID = map[keboola.ComponentID]string{"keboola.ex-aws-s3": "in.c-keboola-ex-aws-s3-"}
	m.defaultBucketsByPrefix = map[string]keboola.ComponentID{"in.c-keboola-ex-aws-s3-": "keboola.ex-aws-s3"}

	defaultBucket, found := m.GetDefaultBucketByComponentID("keboola.ex-aws-s3", "123")
	assert.True(t, found)
	assert.Equal(t, "in.c-keboola-ex-aws-s3-123", defaultBucket)

	defaultBucket, found = m.GetDefaultBucketByComponentID("keboola.ex-google-drive", "123")
	assert.False(t, found)
	assert.Empty(t, defaultBucket)
}
