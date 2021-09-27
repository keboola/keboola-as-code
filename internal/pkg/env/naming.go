package env

import (
	"fmt"
	"strings"
)

const Prefix = "KBC_"

type NamingConvention struct{}

func NewNamingConvention() *NamingConvention {
	return &NamingConvention{}
}

// Replace converts flag name to ENV variable name
// for example "storage-api-host" -> "KBC_STORAGE_API_HOST".
func (*NamingConvention) Replace(flagName string) string {
	if len(flagName) == 0 {
		panic(fmt.Errorf("flag name cannot be empty"))
	}

	return Prefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

func Files() []string {
	// https://github.com/bkeepers/dotenv#what-other-env-files-can-i-use
	return []string{
		".env.development.local",
		".env.test.local",
		".env.production.local",
		".env.local",
		".env.development",
		".env.test",
		".env.production",
		".env",
	}
}
