package local

type VolumesAssignment struct {
	// PerPod defines the quantity of volumes simultaneously utilized per pod and sink.
	// This value also corresponds to the number of slices simultaneously opened per pod and the File.
	// If the specified number of volumes is unavailable, all available volumes will be used.
	// With the growing number of volumes, the per pod throughput increases.
	PerPod int `json:"perPod" configKey:"perPod" configUsage:"Volumes simultaneously utilized per pod and sink." validate:"required,min=1"`
	// PreferredTypes contains a list of preferred volume types,
	// the value is used when assigning volumes to the file slices, see writer.Volumes.VolumesFor.
	// The first value is the most preferred volume type.
	PreferredTypes []string `json:"preferredTypes" configKey:"preferredTypes" configUsage:"List of preferred volume types, start with the most preferred."`
}
