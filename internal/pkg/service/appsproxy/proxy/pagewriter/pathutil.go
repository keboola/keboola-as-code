package pagewriter

func IsStreamlitHealthCheck(path string) bool {
	return path == "/_stcore/health" || path == "/_stcore/host-config"
}
