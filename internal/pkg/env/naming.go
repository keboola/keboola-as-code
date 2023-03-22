package env

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type NamingConvention struct {
	prefix   string
	replacer *strings.Replacer
}

func NewNamingConvention(prefix string) *NamingConvention {
	return &NamingConvention{prefix: prefix, replacer: strings.NewReplacer("-", "_", ".", "_")}
}

// FlagToEnv converts flag name to ENV variable name
// for example "storage-api-host" -> "KBC_STORAGE_API_HOST".
func (n *NamingConvention) FlagToEnv(flagName string) string {
	if len(flagName) == 0 {
		panic(errors.New("flag name cannot be empty"))
	}

	return n.prefix + strings.ToUpper(n.replacer.Replace(flagName))
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
