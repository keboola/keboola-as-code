package utils

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"text/template"

	"go.uber.org/zap"
)

const userFriendlyPanicTmpl = `
---------------------------------------------------
Keboola Connection client had a problem and crashed.

To help us diagnose the problem you can send us a crash report.

{{ if .LogFile -}}
We have generated a log file at "{{.LogFile}}". 

Please submit email to "support@keboola.com" and include the log file as an attachment.
{{- else -}}
Please run the command again with the flag "--log-file <path>" to generate a log file.

Then please submit email to "support@keboola.com" and include the log file as an attachment.
{{- end }}

We take privacy seriously, and do not perform any automated log file collection.

Thank you kindly!`

type UserError struct {
	Message  string
	ExitCode int
}

func (e *UserError) Error() string {
	return e.Message
}

func NewUserError(message string) *UserError {
	return &UserError{message, 1}
}

func NewUserErrorWithCode(exitCode int, message string) *UserError {
	return &UserError{message, exitCode}
}

func ProcessPanic(err interface{}, logger *zap.SugaredLogger, logFilePath string) int {
	switch v := err.(type) {
	case *UserError:
		logger.Debugf("User error panic: %s", v.Message)
		logger.Debugf("Trace:\n" + string(debug.Stack()))
		if len(logFilePath) > 0 {
			logger.Infof("Details can be found in the log file \"%s\".\n", logFilePath)
		}
		return v.ExitCode
	default:
		logger.Debugf("Unexpected panic: %s", err)
		logger.Debugf("Trace:\n" + string(debug.Stack()))
		logger.Info(panicMessage(logFilePath))
		return 1
	}
}

func panicMessage(logFile string) string {
	tmpl, err := template.New("panicMsg").Parse(userFriendlyPanicTmpl)
	if err != nil {
		panic(fmt.Errorf("cannot parse panic template: %w", err))
	}

	var output bytes.Buffer
	err = tmpl.Execute(
		&output,
		struct{ LogFile string }{logFile},
	)
	if err != nil {
		panic(fmt.Errorf("cannot render panic template: %w", err))
	}

	return output.String()
}
