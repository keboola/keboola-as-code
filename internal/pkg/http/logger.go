package http

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-resty/resty/v2"
)

const LoggerPrefix = "HTTP%s\t"

type httpLogger struct {
	client *Client
}

func (l *httpLogger) Debugf(format string, v ...interface{}) {
	l.logWithoutSecretsf("", format, v...)
}

func (l *httpLogger) Warnf(format string, v ...interface{}) {
	l.logWithoutSecretsf("-WARN", format, v...)
}

func (l *httpLogger) Errorf(format string, v ...interface{}) {
	l.logWithoutSecretsf("-ERROR", format, v...)
}

func (l *httpLogger) logWithoutSecretsf(level string, format string, v ...interface{}) {
	v = append([]interface{}{level}, v...)
	msg := fmt.Sprintf(LoggerPrefix+format, v...)
	msg = removeSecrets(msg)
	msg = strings.TrimSuffix(msg, "\n")
	l.client.logger.Debug(msg)
}

func removeSecrets(str string) string {
	return regexp.MustCompile(`(?i)(token[^\w/,]\s*)\d[^\s/]*`).ReplaceAllString(str, "$1*****")
}

func responseToLog(res *resty.Response) string {
	req := res.Request
	return fmt.Sprintf("%s %s | %d | %s", req.Method, urlForLog(req), res.StatusCode(), res.Time())
}

func urlForLog(request *resty.Request) string {
	url := request.URL

	// No response -> url contains placeholders
	if request.RawRequest == nil {
		if pathParams, ok := request.Context().Value(contextKey("pathParams")).(map[string]string); ok {
			for p, v := range pathParams {
				url = strings.ReplaceAll(url, "{"+p+"}", "{"+p+"=\""+v+"\"}")
			}
		}

		if queryParams, ok := request.Context().Value(contextKey("queryParams")).(map[string]string); ok {
			var queryPairs []string
			for p, v := range queryParams {
				queryPairs = append(queryPairs, fmt.Sprintf("%s=\"%s\"", p, v))
			}
			if len(queryPairs) > 0 {
				url += " | query: " + strings.Join(queryPairs, ", ")
			}
		}
	}

	return url
}
