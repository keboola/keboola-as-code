package template

type CreateFlags struct {
	ID             string `mapstructure:"id" usage:"template ID"`
	Name           string `mapstructure:"name" usage:"template name"`
	Description    string `mapstructure:"description" usage:"template description"`
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Branch         string `mapstructure:"branch" shorthand:"b" usage:"branch ID or name"`
	Configs        string `mapstructure:"configs" shorthand:"c" usage:"comma separated list of {componentId}:{configId}"`
	UsedComponents string `mapstructure:"used-components" shorthand:"u" usage:"comma separated list of component ids"`
	AllConfigs     bool   `mapstructure:"all-configs" shorthand:"a" usage:"use all configs from the branch"`
	AllInputs      bool   `mapstructure:"all-inputs" usage:"use all found config/row fields as user inputs"`
}
