package config

import (
	"path"
	"strings"
)

const (
	InternalPrefix  = "/_proxy"
	RequestIDHeader = "X-Request-ID"
)

// IsInternalPath matches the path in every form an app could resolve to the internal prefix:
// any case, with ";" segment parameters, with "//" or ".." left by encoded characters,
// and with ".." the app does not resolve.
func IsInternalPath(p string) bool {
	segments := strings.Split(strings.ToLower(p), "/")
	kept := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment, _, _ = strings.Cut(segment, ";")
		if segment == "" || segment == "." {
			continue
		}
		kept = append(kept, segment)
	}
	unresolved := "/" + strings.Join(kept, "/")
	return hasInternalPrefix(unresolved) || hasInternalPrefix(path.Clean(unresolved))
}

func hasInternalPrefix(p string) bool {
	return p == InternalPrefix || strings.HasPrefix(p, InternalPrefix+"/")
}
