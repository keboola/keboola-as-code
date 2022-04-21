package storageapi

import (
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (a *Api) GetDefaultBranch() (*model.Branch, error) {
	branches, err := a.ListBranches()
	if err != nil {
		return nil, err
	}

	for _, branch := range branches {
		if branch.IsDefault {
			return branch, nil
		}
	}

	return nil, fmt.Errorf("default branch not found")
}

func (a *Api) GetBranch(branchId model.BranchId) (*model.Branch, error) {
	response := a.GetBranchRequest(branchId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Branch), nil
	}
	return nil, response.Err()
}

func (a *Api) CreateBranch(branch *model.Branch) (*model.Job, error) {
	response := a.CreateBranchRequest(branch).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Job), nil
	}
	return nil, response.Err()
}

func (a *Api) UpdateBranch(branch *model.Branch, changed model.ChangedFields) (*model.Branch, error) {
	response := a.UpdateBranchRequest(branch, changed).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Branch), nil
	}
	return nil, response.Err()
}

func (a *Api) ListBranches() ([]*model.Branch, error) {
	response := a.ListBranchesRequest().Send().Response
	if response.HasResult() {
		return *response.Result().(*[]*model.Branch), nil
	}
	return nil, response.Err()
}

func (a *Api) DeleteBranch(key model.BranchKey) (*model.Job, error) {
	response := a.DeleteBranchRequest(key).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Job), nil
	}
	return nil, response.Err()
}

// GetBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/branch-detail
func (a *Api) GetBranchRequest(branchId model.BranchId) *client.Request {
	branch := &model.Branch{}
	return a.
		NewRequest(resty.MethodGet, "dev-branches/{branchId}").
		SetPathParam("branchId", branchId.String()).
		SetResult(branch)
}

// CreateBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/create-branch
func (a *Api) CreateBranchRequest(branch *model.Branch) *client.Request {
	job := &model.Job{}
	// Id is autogenerated
	if branch.Id != 0 {
		panic(fmt.Errorf("branch id is set but it should be auto-generated"))
	}

	// Default branch cannot be created
	if branch.IsDefault {
		panic(fmt.Errorf("default branch cannot be created"))
	}

	// Create request
	request := a.
		NewRequest(resty.MethodPost, "dev-branches").
		SetFormBody(map[string]string{
			"name":        branch.Name,
			"description": branch.Description,
		}).
		SetResult(job)

	request.OnSuccess(waitForJob(a, request, job, func(response *client.Response) {
		// Set branch id from the job results
		branch.Id = model.BranchId(cast.ToInt(job.Results["id"]))
	}))

	return request
}

// UpdateBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/update-branch
func (a *Api) UpdateBranchRequest(branch *model.Branch, changed model.ChangedFields) *client.Request {
	// Id is required
	if branch.Id == 0 {
		panic("branch id must be set")
	}

	// Data
	all := map[string]string{
		"description": branch.Description,
	}

	// Name of the default branch cannot be changed
	if !branch.IsDefault {
		all["name"] = branch.Name
	}

	// Create request
	request := a.
		NewRequest(resty.MethodPut, "dev-branches/{branchId}").
		SetPathParam("branchId", branch.Id.String()).
		SetFormBody(getChangedValues(all, changed)).
		SetResult(branch)

	return request
}

// ListBranchesRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
func (a *Api) ListBranchesRequest() *client.Request {
	branches := make([]*model.Branch, 0)
	return a.
		NewRequest(resty.MethodGet, "dev-branches").
		SetResult(&branches)
}

// DeleteBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/delete-branch
func (a *Api) DeleteBranchRequest(key model.BranchKey) *client.Request {
	job := &model.Job{}
	request := a.
		NewRequest(resty.MethodDelete, "dev-branches/{branchId}").
		SetPathParam("branchId", key.Id.String()).
		SetResult(job)
	request.OnSuccess(waitForJob(a, request, job, nil))
	return request
}

// ListBranchMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/list
func (a *Api) ListBranchMetadataRequest(branchId model.BranchId) *client.Request {
	var metadata []Metadata
	return a.
		NewRequest(resty.MethodGet, "branch/{branchId}/metadata").
		SetPathParam("branchId", branchId.String()).
		SetResult(metadata)
}

// AppendBranchMetadataRequest https://keboola.docs.apiary.io/#reference/metadata/development-branch-metadata/create-or-update
func (a *Api) AppendBranchMetadataRequest(branch *model.Branch) *client.Request {
	// Empty, we have nothing to append
	if len(branch.Metadata) == 0 {
		return nil
	}

	formBody := make(map[string]string)
	i := 0
	for k, v := range branch.Metadata {
		formBody[fmt.Sprintf("metadata[%d][key]", i)] = k
		formBody[fmt.Sprintf("metadata[%d][value]", i)] = v
		i++
	}
	return a.
		NewRequest(resty.MethodPost, "branch/{branchId}/metadata").
		SetPathParam("branchId", branch.Id.String()).
		SetFormBody(formBody)
}
