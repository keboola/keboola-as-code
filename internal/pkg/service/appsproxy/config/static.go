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
// any case, with or without ";" segment parameters, and with ".." either resolved or not.
func IsInternalPath(p string) bool {
	p = strings.ToLower(p)
	for _, candidate := range []string{p, stripSegmentParams(p)} {
		if hasInternalPrefix(dropEmptyAndDotSegments(candidate)) || hasInternalPrefix(path.Clean(candidate)) {
			return true
		}
	}
	return false
}

func stripSegmentParams(p string) string {
	segments := strings.Split(p, "/")
	for i, segment := range segments {
		segments[i], _, _ = strings.Cut(segment, ";")
	}
	return strings.Join(segments, "/")
}

func dropEmptyAndDotSegments(p string) string {
	segments := strings.Split(p, "/")
	kept := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "." {
			continue
		}
		kept = append(kept, segment)
	}
	return "/" + strings.Join(kept, "/")
}

func hasInternalPrefix(p string) bool {
	return p == InternalPrefix || strings.HasPrefix(p, InternalPrefix+"/")
}
