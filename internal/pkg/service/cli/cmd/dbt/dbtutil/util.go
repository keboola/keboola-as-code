package dbtutil

import "strings"

// BaseURLFromHost derives the Keboola Query Service URL from the Storage API host.
// The input scheme (http:// or https://) is preserved; only the hostname prefix is rewritten.
//
// Examples:
//
//	"https://connection.keboola.com"           → "https://query.keboola.com"
//	"https://connection.eu-west-1.keboola.com" → "https://query.eu-west-1.keboola.com"
//	"http://connection.keboola.com"             → "http://query.keboola.com"
//	"connection.keboola.com"                    → "https://query.keboola.com" (no scheme → https assumed)
func BaseURLFromHost(host string) string {
	scheme := "https://"
	bare := host
	switch {
	case strings.HasPrefix(host, "https://"):
		bare = strings.TrimPrefix(host, "https://")
	case strings.HasPrefix(host, "http://"):
		scheme = "http://"
		bare = strings.TrimPrefix(host, "http://")
	}
	return scheme + "query." + strings.TrimPrefix(bare, "connection.")
}
