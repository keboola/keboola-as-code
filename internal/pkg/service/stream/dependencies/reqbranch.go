package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// branchRequestScope implements BranchRequestScope interface.
type branchRequestScope struct {
	ProjectRequestScope
	branch definition.Branch
}

func NewBranchRequestScope(ctx context.Context, prjReqScp ProjectRequestScope, branch key.BranchIDOrDefault) (v BranchRequestScope, err error) {
	ctx, span := prjReqScp.Telemetry().Tracer().Start(ctx, "keboola.go.stream.dependencies.NewBranchRequestScope")
	defer span.End(&err)
	return newBranchRequestScope(ctx, prjReqScp, branch)
}

func newBranchRequestScope(ctx context.Context, prjReqScp ProjectRequestScope, branchInput key.BranchIDOrDefault) (*branchRequestScope, error) {
	d := &branchRequestScope{}
	d.ProjectRequestScope = prjReqScp

	// Get or create branch (in our database)
	var err error
	d.branch, err = getBranch(ctx, prjReqScp, branchInput)
	if err != nil && errors.As(err, &svcerrors.ResourceNotFoundError{}) {
		d.branch, err = createBranch(ctx, prjReqScp, branchInput)
	}
	if err != nil {
		return nil, err
	}

	return d, nil
}

func getBranch(ctx context.Context, d ProjectRequestScope, branchInput key.BranchIDOrDefault) (definition.Branch, error) {
	repo := d.DefinitionRepository().Branch()
	if branchInput.Default() {
		return repo.GetDefault(d.ProjectID()).Do(ctx).ResultOrErr()
	} else if branchID, convErr := branchInput.Int(); convErr == nil {
		branchKey := key.BranchKey{ProjectID: d.ProjectID(), BranchID: keboola.BranchID(branchID)}
		return repo.Get(branchKey).Do(ctx).ResultOrErr()
	} else {
		err := svcerrors.NewBadRequestError(errors.Errorf(`invalid branch: expected "default" or <int>, given "%s"`, branchInput))
		return definition.Branch{}, err
	}
}

func createBranch(ctx context.Context, d ProjectRequestScope, branchInput key.BranchIDOrDefault) (branch definition.Branch, err error) {
	repo := d.DefinitionRepository().Branch()

	var res *keboola.Branch
	if branchInput.Default() {
		res, err = d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
		if err != nil {
			return definition.Branch{}, err
		}
	} else if branchID, convErr := branchInput.Int(); convErr == nil {
		res, err = d.KeboolaProjectAPI().GetBranchRequest(keboola.BranchKey{ID: keboola.BranchID(branchID)}).Send(ctx)
		if err != nil {
			return definition.Branch{}, err
		}
	} else {
		err = svcerrors.NewBadRequestError(errors.Errorf(`invalid branch: expected "default" or <int>, given "%s"`, branchInput))
		return definition.Branch{}, err
	}

	if !res.IsDefault {
		err = svcerrors.NewBadRequestError(errors.Errorf(`currently only default branch is supported, branch "%d" is not default`, res.ID))
		return definition.Branch{}, err
	}

	branchKey := key.BranchKey{ProjectID: d.ProjectID(), BranchID: res.ID}
	branch = definition.Branch{BranchKey: branchKey, IsDefault: true}

	by := definition.ByFromToken(d.StorageAPIToken())
	return repo.Create(&branch, d.Clock().Now(), by).Do(ctx).ResultOrErr()
}

func (v *branchRequestScope) Branch() definition.Branch {
	return v.branch
}

func (v *branchRequestScope) BranchKey() key.BranchKey {
	return v.branch.BranchKey
}
