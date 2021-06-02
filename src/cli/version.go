package cli

import "runtime"

// Defined on build time:

var Version = "dev"
var GitCommit = "-"
var BuildDate = "-"

// version for --version flag
func (c *commander) version() string {
	return "Version:    " + Version + "\n" +
		"Git commit: " + GitCommit + "\n" +
		"Build date: " + BuildDate + "\n" +
		"Go version: " + runtime.Version() + "\n" +
		"Os/Arch:    " + runtime.GOOS + "/" + runtime.GOARCH + "\n"
}
