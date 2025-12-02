package pagewriter

import "strings"

func IsStreamlitHealthCheck(path string) bool {
	return strings.HasSuffix(path, "/_stcore/health") || strings.HasSuffix(path, "/_stcore/host-config")
}
