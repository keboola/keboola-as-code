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
// any case, with ";" segment parameters, and with "//" or ".." left by encoded characters.
func IsInternalPath(p string) bool {
	segments := strings.Split(strings.ToLower(p), "/")
	for i, segment := range segments {
		segments[i], _, _ = strings.Cut(segment, ";")
	}
	p = path.Clean(strings.Join(segments, "/"))
	return p == InternalPrefix || strings.HasPrefix(p, InternalPrefix+"/")
}
