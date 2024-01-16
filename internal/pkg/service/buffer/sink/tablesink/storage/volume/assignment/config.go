package assignment

// Config configures assignment of pod volumes to a File.
type Config struct {
	// Count defines the quantity of volumes simultaneously utilized per sink in the entire cluster.
	// This value also corresponds to the number of slices simultaneously opened per the File.
	// If the specified number of volumes is unavailable, all available volumes will be used.
	// With the growing number of volumes, the per pod throughput increases.
	Count int `json:"count" configKey:"count" configUsage:"Volumes count simultaneously utilized per sink." validate:"required,min=1,max=100"`
	// PreferredTypes contains a list of preferred volume types,
	// the value is used when assigning volumes to the file slices, see writer.Volumes.VolumesFor.
	// The first value is the most preferred volume type.
	PreferredTypes []string `json:"preferredTypes" configKey:"preferredTypes" validate:"min=1" configUsage:"List of preferred volume types, start with the most preferred."`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Count:          1,
		PreferredTypes: []string{"default"},
	}
}
