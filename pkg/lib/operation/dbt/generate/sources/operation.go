package sources

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

type Options struct {
	BranchKey  keboola.BranchKey
	TargetName string
	Buckets    []listbuckets.Bucket // optional, set if the buckets have been loaded in a parent command
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.generate.sources")
	defer span.End(&err)

	// Get dbt project
	project, _, err := d.LocalDbtProject(ctx)
	if err != nil {
		return err
	}
	fs := project.Fs()

	// List bucket, if not set
	o.Buckets, err = listbuckets.Run(ctx, listbuckets.Options{BranchKey: o.BranchKey, TargetName: o.TargetName}, d)
	if err != nil {
		return errors.Errorf("could not list buckets: %w", err)
	}

	if !fs.Exists(ctx, dbt.SourcesPath) {
		err = fs.Mkdir(ctx, dbt.SourcesPath)
		if err != nil {
			return err
		}
	}

	// Group tables by bucket and write file
	for _, bucket := range o.Buckets {
		sourcesDef := generateSourcesDefinition(bucket)

		// Create YAML encoder with proper formatting
		var buf bytes.Buffer
		enc := yaml.NewEncoder(&buf)
		enc.SetIndent(2)

		// Create a YAML node with proper style settings
		var node yaml.Node
		if err := node.Encode(sourcesDef); err != nil {
			return err
		}

		// Set sequence style to not indent
		setNodeStyle(&node)

		// Encode the node
		if err := enc.Encode(&node); err != nil {
			return err
		}

		// Add document separator and ensure single newline at end
		content := "---\n" + strings.TrimSpace(buf.String()) + "\n"

		// Write the file
		err = fs.WriteFile(ctx, filesystem.NewRawFile(fmt.Sprintf("%s/%s.yml", dbt.SourcesPath, bucket.SourceName), content))
		if err != nil {
			return err
		}
	}

	d.Logger().Infof(ctx, `Sources stored in "%s" directory.`, dbt.SourcesPath)
	return nil
}

// setNodeStyle sets the YAML node style for proper formatting
func setNodeStyle(node *yaml.Node) {
	if node.Kind == yaml.MappingNode {
		// For each key-value pair in the mapping
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]

			// If the value is a sequence, set its column to match the key's column
			if value.Kind == yaml.SequenceNode {
				value.Column = key.Column
				for _, item := range value.Content {
					item.Column = key.Column
				}
			}

			// Recursively process the value
			setNodeStyle(value)
		}
	} else if node.Kind == yaml.SequenceNode {
		// If this is a sequence node, ensure all items have the same column as the sequence
		for _, item := range node.Content {
			item.Column = node.Column
			setNodeStyle(item)
		}
	}
}
