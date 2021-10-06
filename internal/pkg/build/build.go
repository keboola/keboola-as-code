//nolint:gochecknoglobals
package build

const DevVersionValue = "dev"

// Defined on build time:

var (
	GitCommit    = "-"
	BuildVersion = DevVersionValue
	BuildDate    = "-"
)
