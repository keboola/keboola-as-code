package version

import (
	"runtime"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
)

const DevVersionValue = "dev"

// Version for --version flag.
func Version() string {
	return "Version:    " + build.BuildVersion + "\n" +
		"Git commit: " + build.GitCommit + "\n" +
		"Build date: " + build.BuildDate + "\n" +
		"Go version: " + runtime.Version() + "\n" +
		"Os/Arch:    " + runtime.GOOS + "/" + runtime.GOARCH + "\n"
}
