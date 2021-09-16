package encryption

import (
	"regexp"
	"strings"
)

func IsKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

func IsEncrypted(value string) bool {
	currentFormatMatch := regexp.MustCompile(`^KBC::(ProjectSecure|ComponentSecure|ConfigSecure)(KV)?::.+$`).MatchString(value)
	legacyFormatMatch := regexp.MustCompile(`^KBC::(Encrypted==|ComponentProjectEncrypted==|ComponentEncrypted==).+$`).MatchString(value)
	return currentFormatMatch || legacyFormatMatch
}
