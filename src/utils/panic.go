package utils

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"os"
	"runtime/debug"
	"text/template"
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

We take privacy seriously, and do not perform any automated error collection.

Thank you kindly!`

type UserError struct {
	message  string
	exitCode int
}

func (e *UserError) Error() string {
	return e.message
}

func NewUserError(message string) *UserError {
	return &UserError{message, 1}
}

func NewUserErrorWithCode(exitCode int, message string) *UserError {
	return &UserError{message, exitCode}
}

func ProcessPanic(err interface{}, logger *zap.SugaredLogger, logFile string) {
	switch v := err.(type) {
	case *UserError:
		logger.Debugf("User error panic: %s", v.message)
		logger.Debugf("Trace:\n" + string(debug.Stack()))
		fmt.Println("Error: " + v.message)
		if len(logFile) > 0 {
			fmt.Printf("Details can be found in the log file \"%s\".\n", logFile)
		}
		os.Exit(v.exitCode)
	default:
		logger.Debugf("Unexpected panic: %s", err)
		logger.Debugf("Trace:\n" + string(debug.Stack()))
		fmt.Println(panicMessage(logFile))
		os.Exit(1)
	}
}

func panicMessage(logFile string) string {
	tmpl, err := template.New("panicMsg").Parse(userFriendlyPanicTmpl)
	if err != nil {
		panic(fmt.Errorf("cannot parse panic template: %s", err))
	}

	var output bytes.Buffer
	err = tmpl.Execute(
		&output,
		struct{ LogFile string }{logFile},
	)
	if err != nil {
		panic(fmt.Errorf("cannot render panic template: %s", err))
	}

	return output.String()
}
