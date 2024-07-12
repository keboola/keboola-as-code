package configmap

import (
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type CustomStringType string

type CustomIntType int

// TestValueNV is a value with Normalize and Validate methods.
type TestValueNV struct {
	ValidationError error
	Foo             string `configKey:"foo"`
}

func (c *TestValueNV) Normalize() {
	c.Foo = strings.TrimSpace(c.Foo)
}

func (c *TestValueNV) Validate() error {
	return c.ValidationError
}

// TestConfigNV is a configuration structure with Normalize and Validate methods.
type TestConfigNV struct {
	ValidationError error
	Key1            TestValueNV `configKey:"key1"`
	Key2            struct {
		KeyA TestValueNV `configKey:"keyA" validate:"required"`
		KeyB TestValueNV `configKey:"keyB" validate:"required"`
	} `configKey:"key2"`
	Normalized string
}

func (c *TestConfigNV) Normalize() {
	c.Normalized = "normalized"
}

func (c *TestConfigNV) Validate() error {
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
	StringWithUsage  string           `configKey:"stringWithUsage" configUsage:"An usage text." validate:"ne=invalid"`
	Duration         time.Duration    `configKey:"duration"`
	DurationNullable *time.Duration   `configKey:"durationNullable"`
	URL              *url.URL         `configKey:"url" configShorthand:"u"`
	Addr             netip.Addr       `configKey:"address"`         // TextUnmarshaler/BinaryUnmarshaler interface
	AddrNullable     *netip.Addr      `configKey:"addressNullable"` // TextUnmarshaler/BinaryUnmarshaler interface
	Nested           Nested           `configKey:"nested"`
	Skipped          bool             `configKey:"-"`
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
	URL              Value[*url.URL]         `configKey:"url" configShorthand:"u"`
	Addr             Value[netip.Addr]       `configKey:"address"`         // TextUnmarshaler/BinaryUnmarshaler interface
	AddrNullable     Value[*netip.Addr]      `configKey:"addressNullable"` // TextUnmarshaler/BinaryUnmarshaler interface
	Nested           NestedValue             `configKey:"nested"`
	Skipped          bool                    `configKey:"-"`
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
