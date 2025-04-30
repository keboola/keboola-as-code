package create

import (
	"context"
	"io"
	"os"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	tmplTest "github.com/keboola/keboola-as-code/internal/pkg/template/test"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

type Options struct {
	TestName string // name of the test
	Inputs   template.InputsValues
}

type dependencies interface {
	Process() *servicectx.Process
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
	Stderr() io.Writer
}

func Run(ctx context.Context, tmpl *template.Template, o Options, d dependencies) (err error) {
	tempDir, err := os.MkdirTemp("", "kac-test-template-") //nolint:forbidigo
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil { // nolint: forbidigo
			d.Logger().Warnf(ctx, `cannot remove temp dir "%s": %w`, tempDir, err)
		}
	}()

	branchID := 1
	prjState, _, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, d.Logger(), d.Telemetry(), tmpl.ProjectsFilePath(), d.Stdout(), d.Stderr(), d.Process(), branchID, false)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(ctx, `Working directory set up.`)

	// Run use template operation
	tmplOpts := useTemplate.Options{
		InstanceName:          "test",
		TargetBranch:          model.BranchKey{ID: keboola.BranchID(branchID)},
		Inputs:                o.Inputs,
		InstanceID:            template.InstanceIDForTest,
		SkipEncrypt:           true,
		SkipSecretsValidation: true,
	}
	opResult, err := useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)
	if err != nil {
		return err
	}

	// Create test files
	err = tmpl.CreateTest(ctx, o.TestName, o.Inputs, prjState, opResult.InstanceID)
	if err != nil {
		return err
	}

	d.Logger().Infof(ctx, "The test was created in folder tests/%s.", o.TestName)

	return nil
}
