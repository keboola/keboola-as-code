package cliconfig_test

type Config struct {
	Ignored         string
	String          string  `mapstructure:"string"`
	Int             int     `mapstructure:"int"`
	Float           float64 `mapstructure:"float"`
	StringWithUsage string  `mapstructure:"string-with-usage" usage:"An usage text."`
	Nested          Nested  `mapstructure:"nested"`
}

type Nested struct {
	Ignored string
	Foo     string `mapstructure:"foo-123"`
	Bar     int    `mapstructure:"bar"`
}
