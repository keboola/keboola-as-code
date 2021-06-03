package version

import "runtime"

// Defined on build time:

var GitCommit = "-"
var BuildVersion = "dev"
var BuildDate = "-"

// Version for --version flag
func Version() string {
	return "Version:    " + BuildVersion + "\n" +
		"Git commit: " + GitCommit + "\n" +
		"Build date: " + BuildDate + "\n" +
		"Go version: " + runtime.Version() + "\n" +
		"Os/Arch:    " + runtime.GOOS + "/" + runtime.GOARCH + "\n"
}
