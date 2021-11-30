package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComponentDefaultBucket(t *testing.T) {
	t.Parallel()

	componentJson := `
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
  "ico32": "https://assets-cdn.keboola.com/developer-portal/icons/keboola.ex-aws-s3/32/1576250595503.png",
  "ico64": "https://assets-cdn.keboola.com/developer-portal/icons/keboola.ex-aws-s3/64/1576250595503.png",
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
	var component Component
	_ = json.Unmarshal([]byte(componentJson), &component)
	assert.Equal(t, "keboola.ex-aws-s3", component.Id)
	assert.Equal(t, "AWS S3", component.Name)
	assert.IsType(t, ComponentData{}, component.Data)
	assert.Equal(t, true, component.Data.DefaultBucket)
	assert.Equal(t, "in", component.Data.DefaultBucketStage)

	componentsMap := NewComponentsMap(nil)
	componentsMap.Set(&component)

	expected := map[string]string{"in.c-keboola-ex-aws-s3-": "keboola.ex-aws-s3"}
	assert.Equal(t, expected, componentsMap.defaultBucketPrefixes)
}

func TestMatchDefaultBucketInTableId(t *testing.T) {
	t.Parallel()

	componentsMap := NewComponentsMap(nil)
	componentsMap.defaultBucketPrefixes = map[string]string{"in.c-keboola-ex-aws-s3-": "keboola.ex-aws-s3"}

	componentId, configId, matchesDefaultBucket := componentsMap.MatchDefaultBucketInTableId("in.c-crm.orders")
	assert.Equal(t, "", componentId)
	assert.Equal(t, "", configId)
	assert.False(t, matchesDefaultBucket)

	componentId, configId, matchesDefaultBucket = componentsMap.MatchDefaultBucketInTableId("in.c-keboola-ex-aws-s3-123456.orders")
	assert.Equal(t, "keboola.ex-aws-s3", componentId)
	assert.Equal(t, "123456", configId)
	assert.True(t, matchesDefaultBucket)

	componentId, configId, matchesDefaultBucket = componentsMap.MatchDefaultBucketInTableId("in.c-keboola-ex-aws-s3.orders")
	assert.Equal(t, "", componentId)
	assert.Equal(t, "", configId)
	assert.False(t, matchesDefaultBucket)
}
