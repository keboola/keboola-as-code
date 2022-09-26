package profile

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Fs() filesystem.Fs
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	TargetName string
}

const profilePath = "profiles.yml"

func Run(ctx context.Context, opts Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.generate.profile")
	defer telemetry.EndSpan(span, &err)

	if !d.Fs().Exists(`dbt_project.yml`) {
		return fmt.Errorf(`missing file "dbt_project.yml" in the current directory`)
	}
	file, err := d.Fs().ReadFile(filesystem.NewFileDef(`dbt_project.yml`))
	if err != nil {
		return err
	}
	configFile := make(map[string]interface{})
	err = yaml.Unmarshal([]byte(file.Content), &configFile)
	if err != nil {
		return err
	}
	profileName, ok := configFile["profile"]
	if !ok {
		return fmt.Errorf(`configuration file "dbt_project.yml" is missing "profile"`)
	}

	logger := d.Logger()
	targetUpper := strings.ToUpper(opts.TargetName)
	profileDetails := map[string]interface{}{
		"target": opts.TargetName,
		"outputs": map[string]interface{}{
			opts.TargetName: map[string]interface{}{
				"account":   fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_ACCOUNT\") }}", targetUpper),
				"database":  fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_DATABASE\") }}", targetUpper),
				"password":  fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_PASSWORD\") }}", targetUpper),
				"schema":    fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_SCHEMA\") }}", targetUpper),
				"type":      fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_TYPE\") }}", targetUpper),
				"user":      fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_USER\") }}", targetUpper),
				"warehouse": fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_WAREHOUSE\") }}", targetUpper),
			},
		},
	}
	profilesFile := make(map[string]interface{})
	profilesFile["send_anonymous_usage_stats"] = false

	if d.Fs().Exists(profilePath) {
		file, err := d.Fs().ReadFile(filesystem.NewFileDef(profilePath))
		if err != nil {
			return err
		}
		err = yaml.Unmarshal([]byte(file.Content), &profilesFile)
		if err != nil {
			return fmt.Errorf(`profiles file "%s" is not valid yaml: %w`, profilePath, err)
		}
	}
	profilesFile[profileName.(string)] = profileDetails

	yamlEnc, err := yaml.Marshal(&profilesFile)
	if err != nil {
		return err
	}
	err = d.Fs().WriteFile(filesystem.NewRawFile(profilePath, string(yamlEnc)))
	if err != nil {
		return err
	}

	logger.Infof(`Profile stored in "%s".`, profilePath)
	return nil
}
