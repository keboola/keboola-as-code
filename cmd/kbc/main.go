package main

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/clifs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd"
)

func main() {
	// Load Os Envs
	osEnvs, err := env.FromOs()
	if err != nil {
		panic(err)
	}

	// Run command
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, osEnvs, clifs.New)
	os.Exit(rootCmd.Execute())
}
