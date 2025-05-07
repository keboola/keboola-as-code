package sources

import (
	"context"
	"fmt"

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

		yamlEnc, err := yaml.Marshal(&sourcesDef)
		if err != nil {
			return err
		}

		err = fs.WriteFile(ctx, filesystem.NewRawFile(fmt.Sprintf("%s/%s.yml", dbt.SourcesPath, bucket.SourceName), string(yamlEnc)))
		if err != nil {
			return err
		}
	}

	d.Logger().Infof(ctx, `Sources stored in "%s" directory.`, dbt.SourcesPath)
	return nil
}
