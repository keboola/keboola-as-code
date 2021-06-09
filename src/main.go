package main

import (
	"keboola-as-code/src/ask"
	"keboola-as-code/src/cli"
	"os"
)

func main() {
	// Run command
	prompt := ask.NewPrompt(os.Stdin, os.Stdout, os.Stderr)
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt)
	cmd.Execute()
}
