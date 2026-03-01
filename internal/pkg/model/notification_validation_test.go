package model

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateNotificationFilters_ValidFields(t *testing.T) {
	t.Parallel()

	validFilters := []keboola.NotificationFilter{
		{Field: "branch.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "123"},
		{Field: "job.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "456"},
		{Field: "job.component.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "ex-generic-v2"},
		{Field: "job.configuration.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "789"},
		{Field: "job.token.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "token-123"},
		{Field: "project.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "proj-456"},
		{Field: "eventType", Operator: keboola.NotificationFilterOperatorEquals, Value: "job-failed"},
	}

	err := ValidateNotificationFilters(validFilters)
	assert.NoError(t, err, "valid filter fields should not return error")
}

func TestValidateNotificationFilters_DeprecatedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		field        string
		correctField string
	}{
		{"configId", "configId", "job.configuration.id"},
		{"componentId", "componentId", "job.component.id"},
		{"branchId", "branchId", "branch.id"},
		{"jobId", "jobId", "job.id"},
		{"tokenId", "tokenId", "job.token.id"},
		{"projectId", "projectId", "project.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filters := []keboola.NotificationFilter{
				{Field: tt.field, Operator: keboola.NotificationFilterOperatorEquals, Value: "123"},
			}

			err := ValidateNotificationFilters(filters)
			require.Error(t, err, "deprecated field should return error")
			assert.Contains(t, err.Error(), "deprecated field name")
			assert.Contains(t, err.Error(), tt.field)
			assert.Contains(t, err.Error(), tt.correctField)
		})
	}
}

func TestValidateNotificationFilters_InvalidFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
	}{
		{"completely_invalid", "foobar"},
		{"another_invalid", "xyz123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filters := []keboola.NotificationFilter{
				{Field: tt.field, Operator: keboola.NotificationFilterOperatorEquals, Value: "123"},
			}

			err := ValidateNotificationFilters(filters)
			require.Error(t, err, "invalid field should return error")
			assert.Contains(t, err.Error(), "invalid field name")
			assert.Contains(t, err.Error(), tt.field)
		})
	}
}

func TestValidateNotificationFilters_EmptyFilters(t *testing.T) {
	t.Parallel()

	err := ValidateNotificationFilters([]keboola.NotificationFilter{})
	assert.NoError(t, err, "empty filters should not return error")
}

func TestValidateNotificationFilters_MultipleFilters(t *testing.T) {
	t.Parallel()

	filters := []keboola.NotificationFilter{
		{Field: "branch.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "123"},
		{Field: "configId", Operator: keboola.NotificationFilterOperatorEquals, Value: "456"}, // deprecated
		{Field: "job.token.id", Operator: keboola.NotificationFilterOperatorEquals, Value: "789"},
	}

	err := ValidateNotificationFilters(filters)
	require.Error(t, err, "should fail on first invalid filter")
	assert.Contains(t, err.Error(), "filter[1]")
	assert.Contains(t, err.Error(), "configId")
}

func TestFindSimilarFieldNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		input               string
		expectedSuggestions []string
	}{
		{
			name:                "config",
			input:               "config",
			expectedSuggestions: []string{"job.configuration.id"},
		},
		{
			name:                "job",
			input:               "job",
			expectedSuggestions: []string{"job.id", "job.component.id", "job.configuration.id", "job.token.id"},
		},
		{
			name:                "branch",
			input:               "branch",
			expectedSuggestions: []string{"branch.id"},
		},
		{
			name:                "completely_invalid",
			input:               "xyz",
			expectedSuggestions: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			suggestions := findSimilarFieldNames(tt.input)

			// Check that all expected suggestions are present
			for _, expected := range tt.expectedSuggestions {
				assert.Contains(t, suggestions, expected, "should contain suggested field")
			}
		})
	}
}
