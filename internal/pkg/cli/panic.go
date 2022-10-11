package cli

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"text/template"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const userFriendlyPanicTmpl = `
---------------------------------------------------
Keboola CLI had a problem and crashed.

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

func ProcessPanic(err interface{}, logger log.Logger, logFilePath string) int {
	logger.Debugf("Unexpected panic: %s", err)
	logger.Debugf("Trace:\n" + string(debug.Stack()))
	logger.Info(panicMessage(logFilePath))
	return 1
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
