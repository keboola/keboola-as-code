package template

type DeleteTemplateFlags struct {
	Branch   string `mapstructure:"branch" usage:"branch ID or name"`
	Instance string `mapstructure:"instance" usage:"instance ID of the template to delete"`
	DryRun   bool   `mapstructure:"dry-run" usage:"print what needs to be done"`
}

type ListTemplateFlag struct {
	Branch string `mapstructure:"branch" usage:"branch ID or name"`
}

type RenameFlags struct {
	Branch   string `mapstructure:"branch" usage:"branch ID or name"`
	Instance string `mapstructure:"instance" usage:"instance ID of the template to delete"`
	NewName  string `mapstructure:"new-name" usage:"new name of the template instance"`
}

type UpgradeTemplateFlags struct {
	Branch     string `mapstructure:"branch" usage:"branch ID or name"`
	Instance   string `mapstructure:"instance" usage:"instance ID of the template to upgrade"`
	Version    string `mapstructure:"version" usage:"target version, default latest stable version"`
	DryRun     bool   `mapstructure:"dry-run" usage:"print what needs to be done"`
	InputsFile string `mapstructure:"inputs-file" usage:"JSON file with inputs values"`
}

type UseTemplateFlags struct {
	Branch       string `mapstructure:"branch" usage:"branch ID or name"`
	InstanceName string `mapstructure:"instance-name" usage:"name of new template instance"`
	InputsFile   string `mapstructure:"inputs-file" usage:"JSON file with inputs values"`
}
