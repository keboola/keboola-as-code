package local

type CreateConfigFlags struct {
	Branch      string `mapstructure:"branch" shorthand:"b" usage:"branch ID or name"`
	ComponentID string `mapstructure:"component-id" shorthand:"c" usage:"component ID"`
	Name        string `mapstructure:"name" shorthand:"n" usage:"name of the new config"`
}

type CreateRowFlags struct {
	Branch string `mapstructure:"branch" shorthand:"b" usage:"branch ID or name"`
	Config string `mapstructure:"config" shorthand:"c" usage:"config name or ID"`
	Name   string `mapstructure:"name" shorthand:"n" usage:"name of the new config row"`
}

type EncryptFlag struct {
	DryRun bool `mapstructure:"dry-run" usage:"print what needs to be done"`
}

type FixPathsFlag struct {
	DryRun bool `mapstructure:"dry-run" usage:"print what needs to be done"`
}

type PersistFlag struct {
	DryRun bool `mapstructure:"dry-run" usage:"print what needs to be done"`
}
