package main

import (
	"os"

	"keboola-as-code/src/cli"
	"keboola-as-code/src/interaction"
)

func main() {
	// Run command
	prompt := interaction.NewPrompt(os.Stdin, os.Stdout, os.Stderr)
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt)
	os.Exit(cmd.Execute())
}
