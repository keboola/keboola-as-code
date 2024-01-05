package log

import (
	"bufio"
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CompareJSONMessages checks that expected json messages appear in actual in the same order.
// Actual string may have extra messages and the rest may have extra fields. String values are compared using wildcards.
// Returns nil if the expectations are met or an error with the first unmatched expected line and all remaining actual lines.
func CompareJSONMessages(expected string, actual string) error {
	expectedScanner := bufio.NewScanner(strings.NewReader(strings.Trim(expected, "\n")))
	actualScanner := bufio.NewScanner(strings.NewReader(strings.Trim(actual, "\n")))

	for expectedScanner.Scan() {
		expectedMessage := expectedScanner.Text()
		var expectedMessageData map[string]any
		err := json.DecodeString(expectedMessage, &expectedMessageData)
		if err != nil {
			return errors.Wrapf(err, "expected string contains invalid json:\n%s", expectedMessage)
		}

		actualMessages := ""
		messageFound := false
		for actualScanner.Scan() {
			actualMessage := actualScanner.Text()
			actualMessages += actualMessage + "\n"
			var actualMessageData map[string]any
			err := json.DecodeString(actualMessage, &actualMessageData)
			if err != nil {
				return errors.Wrapf(err, "actual string contains invalid json:\n%s", actualMessage)
			}

			messageFound = true
			for key, value := range expectedMessageData {
				actualValue, ok := actualMessageData[key]
				if !ok || !valueMatches(value, actualValue) {
					messageFound = false
					break
				}
			}

			if messageFound {
				break
			}
		}

		if !messageFound {
			return errors.Errorf(
				"Expected:\n-----\n%s\n-----\nActual:\n-----\n%s",
				expectedMessage,
				strings.TrimRight(actualMessages, "\n"),
			)
		}
	}

	return nil
}

func valueMatches(value any, actualValue any) bool {
	if expectedString, ok := value.(string); ok {
		if actualString, ok := actualValue.(string); ok {
			err := wildcards.Compare(expectedString, actualString)
			return err == nil
		}

		return false
	}

	return reflect.DeepEqual(actualValue, value)
}

// AssertJSONMessages checks that expected json messages appear in actual in the same order.
// Actual string may have extra messages and the rest may have extra fields. String values are compared using wildcards.
func AssertJSONMessages(t assert.TestingT, expected string, actual string, msgAndArgs ...any) bool {
	err := CompareJSONMessages(expected, actual)
	if err != nil {
		assert.Fail(t, err.Error(), msgAndArgs...)
		return false
	}
	return true
}
