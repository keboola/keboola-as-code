package main

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
)

func main() {
	// Interactive prompt
	prompt := interaction.NewPrompt(os.Stdin, os.Stdout, os.Stderr)

	// Load Os Envs
	osEnvs, err := env.FromOs()
	if err != nil {
		panic(err)
	}

	// Run command
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt, osEnvs, aferofs.NewLocalFsFindProjectDir)
	os.Exit(cmd.Execute())
}
