package main

import (
	"keboola-as-code/src/cli"
	"os"
)

func main() {
	// Run command
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	cmd.Execute()
}
