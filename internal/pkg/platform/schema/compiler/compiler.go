package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

//go:generate go run -mod=mod compiler.go
func main() {
	config := &gen.Config{
		Schema:  "../",
		Target:  "../../model",
		Package: "github.com/keboola/keboola-as-code/internal/pkg/platform/model",
	}

	options := []entc.Option{
		entc.Extensions(primarykey.Extension{}), // process primary key mixins
	}

	if err := clearDir(config.Target); err != nil {
		log.Fatal(err)
	}

	if err := primarykey.GenerateKeys(config); err != nil {
		log.Fatal(err)
	}

	if err := entc.Generate(config.Schema, config, options...); err != nil {
		log.Fatal(err)
	}
}

func clearDir(dir string) error {
	items, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, item := range items {
		// Skip hidden files
		if strings.HasPrefix(item.Name(), ".") {
			continue
		}

		err := os.RemoveAll(filepath.Join(dir, item.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}
