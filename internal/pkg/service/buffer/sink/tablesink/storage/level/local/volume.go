package local

const VolumeIDFile = "volume-id"

type VolumesConfig struct {
	// Count defines the quantity of volumes simultaneously utilized per sink in the entire cluster.
	// This value also corresponds to the number of slices simultaneously opened per the File.
	// If the specified number of volumes is unavailable, all available volumes will be used.
	// With the growing number of volumes, the per pod throughput increases.
	Count int `json:"count" configKey:"count" configUsage:"Volumes count simultaneously utilized per sink." validate:"required,min=1,max=100"`
	// PreferredTypes contains a list of preferred volume types,
	// the value is used when assigning volumes to the file slices, see writer.Volumes.VolumesFor.
	// The first value is the most preferred volume type.
	PreferredTypes []string `json:"preferredTypes" configKey:"preferredTypes" configUsage:"List of preferred volume types, start with the most preferred."`
	// RegistrationTTLSeconds defines number of seconds after the volume registration expires if the node is not available.
	// The mechanism is implemented via etcd lease read more: https://etcd.io/docs/v3.5/learning/api/#lease-api
	RegistrationTTLSeconds int `json:"registrationTimeToLive" configKey:"registrationTTL" configUsage:"Number of seconds after the volume registration expires if the node is not available." validate:"required,min=1,max=60"`
}
