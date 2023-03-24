package cliconfig_test

import (
	"net/netip"
	"net/url"
	"time"
)

type Config struct {
	Embedded         `mapstructure:",squash"`
	Ignored          string
	String           string         `mapstructure:"string"`
	Int              int            `mapstructure:"int"`
	Float            float64        `mapstructure:"float"`
	StringWithUsage  string         `mapstructure:"string-with-usage" usage:"An usage text."`
	Duration         time.Duration  `mapstructure:"duration"`
	DurationNullable *time.Duration `mapstructure:"duration-nullable"`
	URL              *url.URL       `mapstructure:"url"`
	Addr             netip.Addr     `mapstructure:"address"`          // TextUnmarshaler/BinaryUnmarshaler interface
	AddrNullable     *netip.Addr    `mapstructure:"address-nullable"` // TextUnmarshaler/BinaryUnmarshaler interface
	Nested           Nested         `mapstructure:"nested"`
}

type Embedded struct {
	EmbeddedField string `mapstructure:"embedded"`
}

type Nested struct {
	Ignored string
	Foo     string `mapstructure:"foo-123"`
	Bar     int    `mapstructure:"bar"`
}

func (c Config) Normalize() {
	// nop
}

func (c Config) Validate() error {
	// nop
	return nil
}
