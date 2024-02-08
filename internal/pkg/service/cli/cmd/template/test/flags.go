package test

type CreateFlags struct {
	TestName   string `mapstructure:"test-name" usage:"name of the test to be created"`
	InputsFile string `mapstructure:"inputs-file" shorthand:"f" usage:"JSON file with inputs values"`
	Verbose    bool   `mapstructure:"verbose" usage:"show details about creating test"`
}

type RunFlags struct {
	TestName   string `mapstructure:"test-name" usage:"name of a single test to be run"`
	LocalOnly  bool   `mapstructure:"local-only" usage:"run a local test only"`
	RemoteOnly bool   `mapstructure:"remote-only" usage:"run a remote test only"`
	Verbose    bool   `mapstructure:"verbose" usage:"show details about running tests"`
}
