package twinformat

import (
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
)

func TestPlatformToLanguage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		platform string
		expected string
	}{
		{name: "python platform", platform: PlatformPython, expected: LanguagePython},
		{name: "r platform", platform: PlatformR, expected: LanguageR},
		{name: "snowflake platform", platform: PlatformSnowflake, expected: LanguageSQL},
		{name: "bigquery platform", platform: PlatformBigQuery, expected: LanguageSQL},
		{name: "dbt platform", platform: PlatformDBT, expected: LanguageSQL},
		{name: "unknown platform", platform: PlatformUnknown, expected: LanguageSQL},
		{name: "empty platform", platform: "", expected: LanguageSQL},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := platformToLanguage(tc.platform)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucketID string
		expected string
	}{
		{name: "input bucket with c- prefix", bucketID: "in.c-shopify", expected: "shopify"},
		{name: "output bucket with c- prefix", bucketID: "out.c-transformed", expected: "transformed"},
		{name: "bucket without c- prefix", bucketID: "in.shopify", expected: "shopify"},
		{name: "complex bucket name", bucketID: "in.c-google-ads-data", expected: "google-ads-data"},
		{name: "single part", bucketID: "bucket", expected: "bucket"},
		{name: "empty string", bucketID: "", expected: ""},
		{name: "malformed c- only", bucketID: "in.c-", expected: "in.c-"}, // c- without name returns original
		{name: "three parts", bucketID: "in.c-bucket.extra", expected: "bucket"},
		{name: "missing bucket part", bucketID: "in.", expected: "in."}, // malformed returns original
		{name: "whitespace only", bucketID: "   ", expected: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractBucketName(tc.bucketID)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatJobTimePtr(t *testing.T) {
	t.Parallel()

	// Create a fixed time for testing
	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	iso8601Time := iso8601.Time{Time: fixedTime}

	tests := []struct {
		name     string
		time     *iso8601.Time
		expected string
	}{
		{name: "nil time", time: nil, expected: ""},
		{name: "valid time", time: &iso8601Time, expected: "2024-01-15T10:30:00Z"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := formatJobTimePtr(tc.time)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsJobNewer(t *testing.T) {
	t.Parallel()

	// Create test times
	olderTime := iso8601.Time{Time: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)}
	newerTime := iso8601.Time{Time: time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)}

	tests := []struct {
		name          string
		jobStartTime  *iso8601.Time
		existingStart *iso8601.Time
		expected      bool
	}{
		{
			name:          "both nil - keep existing",
			jobStartTime:  nil,
			existingStart: nil,
			expected:      false,
		},
		{
			name:          "job nil, existing has time - job is older",
			jobStartTime:  nil,
			existingStart: &olderTime,
			expected:      false,
		},
		{
			name:          "job has time, existing nil - job is newer",
			jobStartTime:  &olderTime,
			existingStart: nil,
			expected:      true,
		},
		{
			name:          "job is newer",
			jobStartTime:  &newerTime,
			existingStart: &olderTime,
			expected:      true,
		},
		{
			name:          "job is older",
			jobStartTime:  &olderTime,
			existingStart: &newerTime,
			expected:      false,
		},
		{
			name:          "same time - not newer",
			jobStartTime:  &olderTime,
			existingStart: &olderTime,
			expected:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			job := &keboola.QueueJob{StartTime: tc.jobStartTime}
			existing := &keboola.QueueJob{StartTime: tc.existingStart}
			result := isJobNewer(job, existing)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildJobMap(t *testing.T) {
	t.Parallel()

	// Create test times
	olderTime := iso8601.Time{Time: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)}
	newerTime := iso8601.Time{Time: time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)}

	t.Run("empty jobs", func(t *testing.T) {
		t.Parallel()
		result := buildJobMap(nil)
		assert.Empty(t, result)
	})

	t.Run("single job", func(t *testing.T) {
		t.Parallel()
		jobs := []*keboola.QueueJob{
			{
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    "123",
				StartTime:   &olderTime,
			},
		}
		result := buildJobMap(jobs)
		assert.Len(t, result, 1)
		assert.Equal(t, jobs[0], result["keboola.snowflake-transformation:123"])
	})

	t.Run("multiple jobs same config - keeps newer", func(t *testing.T) {
		t.Parallel()
		olderJob := &keboola.QueueJob{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &olderTime,
			Status:      "success",
		}
		newerJob := &keboola.QueueJob{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &newerTime,
			Status:      "error",
		}
		jobs := []*keboola.QueueJob{olderJob, newerJob}
		result := buildJobMap(jobs)
		assert.Len(t, result, 1)
		assert.Equal(t, "error", result["keboola.snowflake-transformation:123"].Status)
	})

	t.Run("multiple configs", func(t *testing.T) {
		t.Parallel()
		job1 := &keboola.QueueJob{
			ComponentID: "keboola.snowflake-transformation",
			ConfigID:    "123",
			StartTime:   &olderTime,
		}
		job2 := &keboola.QueueJob{
			ComponentID: "keboola.python-transformation",
			ConfigID:    "456",
			StartTime:   &newerTime,
		}
		jobs := []*keboola.QueueJob{job1, job2}
		result := buildJobMap(jobs)
		assert.Len(t, result, 2)
		assert.Equal(t, job1, result["keboola.snowflake-transformation:123"])
		assert.Equal(t, job2, result["keboola.python-transformation:456"])
	})
}

func TestLanguageConstants(t *testing.T) {
	t.Parallel()

	// Verify language constants have expected values
	assert.Equal(t, "python", LanguagePython)
	assert.Equal(t, "r", LanguageR)
	assert.Equal(t, "sql", LanguageSQL)
}
