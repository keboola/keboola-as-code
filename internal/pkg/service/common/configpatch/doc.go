// Package configpatch provides tools to replace part of a configuration structure from a patch structure.
//
// Fields are matched by a name tag value, see the WithNameTag option.
//
// Fields marked with a protected tag can only be modified if the WithModifyProtected option is used.
// Name of the protected tag can be modified with the WithProtectedTag option.
//
// # Example structures
//
//	type Config struct {
//	  Foo string `configKey:"foo"`
//	  Bar int    `configKey:"bar" protected:"true" validation:"required"`
//	}
//
//	type ConfigPatch struct {
//	  Foo *string `json:"foo"`
//	  Bar *int    `json:"bar"`
//	}
package configpatch
