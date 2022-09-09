package create

import (
	"context"
	"os"

	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	tmplTest "github.com/keboola/keboola-as-code/internal/pkg/template/test"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

type Options struct {
	TestName string // name of the test
	Inputs   template.InputsValues
}

type dependencies interface {
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, tmpl *template.Template, o Options, d dependencies) (err error) {
	tempDir, err := os.MkdirTemp("", "kac-test-template-") //nolint:forbidigo
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil { // nolint: forbidigo
			d.Logger().Warnf(`cannot remove temp dir "%s": %w`, tempDir, err)
		}
	}()

	branchID := 1
	prjState, _, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, d.Tracer(), d.Logger(), branchID, false)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(`Working directory set up.`)

	test, err := tmpl.CreateTest(o.TestName, o.Inputs, prjState.Fs())
	if err != nil {
		return err
	}

	// Save gathered inputs to the template test inputs.json
	err = test.SaveInputs()
	if err != nil {
		return err
	}

	// Run use template operation
	tmplOpts := useTemplate.Options{
		InstanceName: "test",
		TargetBranch: model.BranchKey{Id: storageapi.BranchID(branchID)},
		Inputs:       o.Inputs,
	}
	_, _, err = useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)
	if err != nil {
		return err
	}

	// Save output from use template operation to the template test
	err = test.SaveExpectedOutput()
	if err != nil {
		return err
	}

	d.Logger().Infof("The test was created in folder tests/%s.", o.TestName)

	return nil
}
