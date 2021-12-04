//nolint:gochecknoglobals
package build

const (
	DevVersionValue = "dev"
	MajorVersion    = 2
)

// Defined on build time:

var (
	GitCommit    = "-"
	BuildVersion = DevVersionValue
	BuildDate    = "-"
)
