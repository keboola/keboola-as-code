package preview

import "strings"

const maxLogValueLength = 64

func SanitizeClaimForLog(value string) string {
	var b strings.Builder
	for i, r := range value {
		if i >= maxLogValueLength {
			b.WriteString("...")
			break
		}
		if isLogSafe(r) {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func isLogSafe(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
}
