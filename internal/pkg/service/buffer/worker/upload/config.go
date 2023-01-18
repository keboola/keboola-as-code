package upload

type config struct {
	CloseSlices  bool
	UploadSlices bool
}

type Option func(c *config)

func newConfig(ops []Option) config {
	c := config{
		CloseSlices:  true,
		UploadSlices: true,
	}
	for _, o := range ops {
		o(&c)
	}
	return c
}

// WithCloseSlices enables/disables the close slices task.
func WithCloseSlices(v bool) Option {
	return func(c *config) {
		c.CloseSlices = v
	}
}

// WithUploadSlices enables/disables the upload slices task.
func WithUploadSlices(v bool) Option {
	return func(c *config) {
		c.UploadSlices = v
	}
}
