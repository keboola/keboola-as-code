package schema

import . "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"

const configsPrefix = Prefix("config/")

type ConfigsRoot struct {
	prefix
}

func Configs() ConfigsRoot {
	return ConfigsRoot{prefix: configsPrefix}
}
