package model

import (
	"slices"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func validNotificationFilterFields() []string {
	return []string{
		"branch.id",
		"job.id",
		"job.component.id",
		"job.configuration.id",
		"job.token.id",
		"project.id",
		"eventType",
	}
}

func deprecatedNotificationFilterFieldNames() map[string]string {
	return map[string]string{
		"configId":      "job.configuration.id",
		"componentId":   "job.component.id",
		"branchId":      "branch.id",
		"jobId":         "job.id",
		"tokenId":       "job.token.id",
		"projectId":     "project.id",
		"configuration": "job.configuration.id",
		"component":     "job.component.id",
		"branch":        "branch.id",
		"job":           "job.id",
		"token":         "job.token.id",
		"project":       "project.id",
	}
}

// ValidateNotificationFilters validates that all filter field names are valid.
// It returns an error if any deprecated or invalid field names are found.
func ValidateNotificationFilters(filters []keboola.NotificationFilter) error {
	for i, filter := range filters {
		if err := validateFilterField(filter.Field, i); err != nil {
			return err
		}
	}
	return nil
}

func validateFilterField(fieldName string, index int) error {
	validFields := validNotificationFilterFields()
	deprecatedFields := deprecatedNotificationFilterFieldNames()

	// Check if field is valid
	if slices.Contains(validFields, fieldName) {
		return nil
	}

	// Check if it's a deprecated field name
	if correctName, ok := deprecatedFields[fieldName]; ok {
		return errors.Errorf(
			`filter[%d] uses deprecated field name "%s". Use "%s" instead`,
			index,
			fieldName,
			correctName,
		)
	}

	// Field name is invalid - suggest alternatives
	suggestions := findSimilarFieldNames(fieldName)
	if len(suggestions) > 0 {
		return errors.Errorf(
			`filter[%d] has invalid field name "%s". Did you mean one of: %s?`,
			index,
			fieldName,
			strings.Join(suggestions, ", "),
		)
	}

	return errors.Errorf(
		`filter[%d] has invalid field name "%s". Valid field names are: %s`,
		index,
		fieldName,
		strings.Join(validFields, ", "),
	)
}

func findSimilarFieldNames(input string) []string {
	validFields := validNotificationFilterFields()
	deprecatedFields := deprecatedNotificationFilterFieldNames()

	var suggestions []string
	lowerInput := strings.ToLower(input)

	// Check if input contains any part of valid field names
	for _, validField := range validFields {
		lowerValid := strings.ToLower(validField)
		// If input contains part of valid field, or valid field contains part of input
		if strings.Contains(lowerValid, lowerInput) || strings.Contains(lowerInput, lowerValid) {
			suggestions = append(suggestions, validField)
		}
	}

	// Also check deprecated names that contain the input
	for deprecatedName, correctName := range deprecatedFields {
		if strings.Contains(strings.ToLower(deprecatedName), lowerInput) && !slices.Contains(suggestions, correctName) {
			suggestions = append(suggestions, correctName)
		}
	}

	return suggestions
}
