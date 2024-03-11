// Package configpatch provides tools to replace part of a configuration structure from a patch structure.
//
// Fields are matched by a name tag value, see the WithNameTag option, default values are "configKey" and "json".
//
// All fields are by default marked as protected.
// Protected fields can only be modified if the WithModifyProtected option is used in the Apply method.
// Individual field can be marked as modifiable by a normal user with the WithModificationAllowedTag option, default value is "modAllowed".
//
// # Example structures
//
//	type Config struct {
//	  Foo string `configKey:"foo" configUsage:"usage" modAllowed:"true"`
//	  Bar int    `configKey:"bar" validate:"required"`
//	}
//
//	type ConfigPatch struct {
//	  Foo *string `json:"foo"`
//	  Bar *int    `json:"bar"`
//	}
package configpatch
