package registration

// Config configures registry of active volumes.
type Config struct {
	// TTLSeconds defines number of seconds after the volume registration expires if the node is not available.
	// The mechanism is implemented via etcd lease read more: https://etcd.io/docs/v3.6/learning/api/#lease-api
	TTLSeconds int `json:"ttlSeconds" configKey:"ttlSeconds" configUsage:"Number of seconds after the volume registration expires if the node is not available." validate:"required,min=1,max=60"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		TTLSeconds: 10,
	}
}
