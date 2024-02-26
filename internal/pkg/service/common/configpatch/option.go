package configpatch

type Option func(*config)

type config struct {
	// modifyProtected enables modification of configuration fields tagged as protected from a patch.
	modifyProtected bool
	// nameTags field provides tags which may contain the field name.
	nameTags []string
	// protectedTag is a tag that marks the field on which it is defined as protected.
	protectedTag      string
	protectedTagValue string
}

func newConfig(opts []Option) config {
	cfg := config{
		modifyProtected:   false,
		nameTags:          []string{"configKey", "json"},
		protectedTag:      "protected",
		protectedTagValue: "true",
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithModifyProtected enables modification of configuration fields tagged as protected from a patch.
func WithModifyProtected() Option {
	return func(c *config) {
		c.modifyProtected = true
	}
}

// WithNameTag sets the tags which may contain the field name.
func WithNameTag(v ...string) Option {
	return func(c *config) {
		c.nameTags = v
	}
}

// WithProtectedTag sets the tag that marks the field on which it is defined as protected.
func WithProtectedTag(v string) Option {
	return func(c *config) {
		c.protectedTag = v
	}
}
