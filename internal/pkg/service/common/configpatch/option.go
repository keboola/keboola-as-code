package configpatch

type Option func(*config)

type config struct {
	// modProtected enables modification of configuration fields without modAllowedTag.
	modProtected bool
	// nameTags field provides tags which may contain the field name.
	nameTags []string
	// modAllowedTag is a tag that marks the field on which it is defined as protected.
	modAllowedTag   string
	modAllowedValue string
}

func newConfig(opts []Option) config {
	cfg := config{
		modProtected:    false,
		nameTags:        []string{"configKey", "json"},
		modAllowedTag:   "modAllowed",
		modAllowedValue: "true",
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithModifyProtected enables modification of configuration fields without modAllowedTag.
// Use this option if the user is super-admin, or it is used by an internal operation.
func WithModifyProtected() Option {
	return func(c *config) {
		c.modProtected = true
	}
}

// WithNameTag sets the tags which may contain the field name.
func WithNameTag(v ...string) Option {
	return func(c *config) {
		c.nameTags = v
	}
}

// WithModificationAllowedTag sets the tag that marks the key on which it is defined can be modified by a normal user.
func WithModificationAllowedTag(v string) Option {
	return func(c *config) {
		c.modAllowedTag = v
	}
}
