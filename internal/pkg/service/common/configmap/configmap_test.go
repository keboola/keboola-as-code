package configmap

import (
	"net/netip"
	"net/url"
	"time"
)

type CustomStringType string

type CustomIntType int

type TestConfigWithValidationError struct {
	Foo             string `configKey:"foo"`
	ValidationError error
}

func (c TestConfigWithValidationError) Normalize() {
	// nop
}

func (c TestConfigWithValidationError) Validate() error {
	return c.ValidationError
}

type TestConfig struct {
	Embedded         `configKey:",squash"`
	Ignored          string
	CustomString     CustomStringType `configKey:"customString"`
	CustomInt        CustomIntType    `configKey:"customInt"`
	SensitiveString  string           `configKey:"sensitiveString" sensitive:"true"`
	StringSlice      []string         `configKey:"stringSlice"`
	Int              int              `configKey:"int"`
	IntSlice         []int            `configKey:"intSlice"`
	Float            float64          `configKey:"float"`
	StringWithUsage  string           `configKey:"stringWithUsage" configUsage:"An usage text."`
	Duration         time.Duration    `configKey:"duration"`
	DurationNullable *time.Duration   `configKey:"durationNullable"`
	URL              *url.URL         `configKey:"url"`
	Addr             netip.Addr       `configKey:"address"`         // TextUnmarshaler/BinaryUnmarshaler interface
	AddrNullable     *netip.Addr      `configKey:"addressNullable"` // TextUnmarshaler/BinaryUnmarshaler interface
	Nested           Nested           `configKey:"nested"`
}

func (c TestConfig) Normalize() {
	// nop
}

func (c TestConfig) Validate() error {
	// nop
	return nil
}

type TestConfigWithValueStruct struct {
	EmbeddedValue    `configKey:",squash"`
	Ignored          Value[string]
	CustomString     Value[CustomStringType] `configKey:"customString"`
	CustomInt        Value[CustomIntType]    `configKey:"customInt"`
	SensitiveString  Value[string]           `configKey:"sensitiveString" sensitive:"true"`
	StringSlice      Value[[]string]         `configKey:"stringSlice"`
	Int              Value[int]              `configKey:"int"`
	IntSlice         Value[[]int]            `configKey:"intSlice"`
	Float            Value[float64]          `configKey:"float"`
	StringWithUsage  Value[string]           `configKey:"stringWithUsage" configUsage:"An usage text."`
	Duration         Value[time.Duration]    `configKey:"duration"`
	DurationNullable Value[*time.Duration]   `configKey:"durationNullable"`
	URL              Value[*url.URL]         `configKey:"url"`
	Addr             Value[netip.Addr]       `configKey:"address"`         // TextUnmarshaler/BinaryUnmarshaler interface
	AddrNullable     Value[*netip.Addr]      `configKey:"addressNullable"` // TextUnmarshaler/BinaryUnmarshaler interface
	Nested           NestedValue             `configKey:"nested"`
}

func (c TestConfigWithValueStruct) Normalize() {
	// nop
}

func (c TestConfigWithValueStruct) Validate() error {
	// nop
	return nil
}

type Embedded struct {
	EmbeddedField string `configKey:"embedded"`
}

type EmbeddedValue struct {
	EmbeddedField Value[string] `configKey:"embedded"`
}

type Nested struct {
	Ignored string
	Foo     string `configKey:"foo"`
	Bar     int    `configKey:"bar"`
}

type NestedValue struct {
	Ignored string
	Foo     Value[string] `configKey:"foo"`
	Bar     Value[int]    `configKey:"bar"`
}
