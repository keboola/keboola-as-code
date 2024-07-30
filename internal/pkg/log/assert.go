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
// Actual string may have extra debug and info messages and the rest may have extra fields. String values are compared using wildcards.
// Returns nil if the expectations are met or an error with the first unmatched expected line and all remaining actual lines.
func CompareJSONMessages(expected string, actual string) error {
	expectedScanner := bufio.NewScanner(strings.NewReader(strings.Trim(expected, "\n")))
	actualScanner := bufio.NewScanner(strings.NewReader(strings.Trim(actual, "\n")))

	for expectedScanner.Scan() {
		expectedMessage := expectedScanner.Text()
		if strings.TrimSpace(expectedMessage) == "" {
			continue
		}

		expectedMessageData, err := decodeMessage(expectedMessage, "expected")
		if err != nil {
			return err
		}

		actualMessages := ""
		messageFound := false
		for actualScanner.Scan() {
			actualMessage := actualScanner.Text()
			actualMessages += actualMessage + "\n"
			actualMessageData, err := decodeMessage(actualMessage, "actual")
			if err != nil {
				return err
			}

			messageFound = true
			for key, value := range expectedMessageData {
				actualValue, ok := actualMessageData[key]
				if !ok || !valueMatches(value, actualValue) {
					messageFound = false
					break
				}
			}

			if !messageFound {
				// Return error, if the actual message cannot be skipped and is not expected
				if err = checkMessageCannotBeSkipped(actualMessage, actualMessageData, expectedMessage); err != nil {
					return err
				}

				// Message not found, skip actual message and check the next one
				continue
			}

			// Message found, go to the next expected message
			break
		}

		if !messageFound {
			return errors.Errorf(
				"Expected:\n-----\n%s\n-----\nActual:\n-----\n%s",
				expectedMessage,
				strings.TrimRight(actualMessages, "\n"),
			)
		}
	}

	// Scan remaining messages, whether there is something important that cannot be skipped
	for actualScanner.Scan() {
		actualMessage := actualScanner.Text()
		actualMessageData, err := decodeMessage(actualMessage, "actual")
		if err != nil {
			return err
		}
		if err = checkMessageCannotBeSkipped(actualMessage, actualMessageData, "<nothing>"); err != nil {
			return err
		}
	}

	return nil
}

func decodeMessage(message, messageType string) (out map[string]any, err error) {
	err = json.DecodeString(message, &out)
	if err != nil {
		return nil, errors.Wrapf(err, "%s string contains invalid json:\n%s", messageType, message)
	}
	return out, nil
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

func checkMessageCannotBeSkipped(actualMessage string, actualData map[string]any, expectedMessage string) error {
	// Messages with warning level and higher must always be defined in expected messages.
	// This prevents issues from not being caught in the tests.
	level, ok := actualData["level"]
	if cannotBeSkipped := ok && level != "debug" && level != "info"; cannotBeSkipped {
		return errors.Errorf(
			"Expected:\n-----\n%s\n-----\nFound unexpected message:\n-----\n%s",
			expectedMessage,
			strings.TrimRight(actualMessage, "\n"),
		)
	}
	return nil
}

type tHelper interface {
	Helper()
}

// AssertJSONMessages checks that expected json messages appear in actual in the same order.
// Actual string may have extra messages and the rest may have extra fields. String values are compared using wildcards.
func AssertJSONMessages(t assert.TestingT, expected string, actual string, msgAndArgs ...any) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}

	err := CompareJSONMessages(expected, actual)
	if err != nil {
		assert.Fail(t, err.Error(), msgAndArgs...)
		return false
	}
	return true
}
