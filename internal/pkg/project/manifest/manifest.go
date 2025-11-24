package manifest

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ProjectIDOverrideENV = "KBC_PROJECT_ID"
	BranchIDOverrideENV  = "KBC_BRANCH_ID"
)

type InvalidManifestError struct {
	error
}

func (e InvalidManifestError) Unwrap() error {
	return e.error
}

func (e InvalidManifestError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
	// Format underlying error
	w.WriteErrorLevel(level, e.error, trace)
}

type records = manifest.Records

// Manifest of the project directory
// file contains IDs and paths of the all objects: branches, configs, rows.
type Manifest struct {
	*records
	// allowTargetENV allows usage KBC_PROJECT_ID and KBC_BRANCH_ID envs to override manifest values
	allowTargetENV bool
	// mapping between manifest representation and memory representation
	mapping        []mappingItem
	project        Project
	naming         naming.Template
	filter         model.ObjectsFilter
	repositories   []model.TemplateRepository
	vaultVariables []*keboola.VaultVariable
}

type Project struct {
	ID      keboola.ProjectID `json:"id" validate:"required"`
	APIHost string            `json:"apiHost" validate:"required"`
}

type mappingItem struct {
	ManifestValue any
	MemoryValue   any
}

func New(projectID keboola.ProjectID, apiHost string) *Manifest {
	// The "http://" protocol can be used in the API host
	// Default HTTPS protocol is stripped, to keep backward compatibility.
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return &Manifest{
		records:      manifest.NewRecords(model.SortByID),
		project:      Project{ID: projectID, APIHost: apiHost},
		naming:       naming.TemplateWithoutIds(),
		filter:       model.NoFilter(),
		repositories: []model.TemplateRepository{repository.DefaultRepository(), repository.ComponentsRepository()},
	}
}

func Load(ctx context.Context, logger log.Logger, fs filesystem.Fs, envs env.Provider, ignoreErrors bool) (*Manifest, error) {
	// Load file content
	content, err := loadFile(ctx, fs)
	if err != nil && (!ignoreErrors || content == nil) {
		return nil, InvalidManifestError{err}
	}

	// Override ProjectID and BranchID by ENVs, if enabled
	var mapping []mappingItem
	if content.AllowTargetENV {
		projectIDStr := envs.Get(ProjectIDOverrideENV)
		branchIDStr := envs.Get(BranchIDOverrideENV)

		if branchIDStr != "" && len(content.Branches) != 1 {
			return nil, errors.Errorf(`env %s=%s can be used if there is one branch in the manifest, found %d branches`, BranchIDOverrideENV, branchIDStr, len(content.Branches))
		}

		if projectIDStr != "" {
			if projectIDInt, err := strconv.Atoi(projectIDStr); err == nil {
				projectID := keboola.ProjectID(projectIDInt)
				if projectID != content.Project.ID {
					logger.Infof(ctx, `Overriding the project ID by the environment variable %s=%v`, ProjectIDOverrideENV, projectID)
					mapping = append(mapping, mappingItem{
						ManifestValue: content.Project.ID,
						MemoryValue:   projectID,
					})
				}
			} else {
				return nil, errors.Errorf(`env %s=%s is not valid project ID`, ProjectIDOverrideENV, projectIDStr)
			}
		}

		if branchIDStr != "" {
			if branchIDInt, err := strconv.Atoi(branchIDStr); err == nil {
				originalBranchID := content.Branches[0].ID
				replacedBranchID := keboola.BranchID(branchIDInt)
				if replacedBranchID != content.Branches[0].ID {
					logger.Infof(ctx, `Overriding the branch ID by the environment variable %s=%v`, BranchIDOverrideENV, replacedBranchID)
					// Map branch ID in all objects
					mapping = append(mapping, mappingItem{
						ManifestValue: originalBranchID,
						MemoryValue:   replacedBranchID,
					})
					// Map allowed branches filter
					mapping = append(mapping, mappingItem{
						ManifestValue: model.AllowedBranch(originalBranchID.String()),
						MemoryValue:   model.AllowedBranch(replacedBranchID.String()),
					})
				}
				// Replace main branch in the filter, with branch ID, if needed
				if len(content.AllowedBranches) == 1 && content.AllowedBranches[0] == model.MainBranchDef {
					content.AllowedBranches[0] = model.AllowedBranch(replacedBranchID.String())
				}
			} else {
				return nil, errors.Errorf(`env %s=%s is not valid branch ID`, BranchIDOverrideENV, branchIDStr)
			}
		}
	}

	// Map manifest IDs to memory IDs
	if len(mapping) > 0 {
		content = deepcopy.CopyTranslate(content, func(original, clone reflect.Value, path deepcopy.Path) {
			for _, pair := range mapping {
				if original.Interface() == pair.ManifestValue {
					clone.Set(reflect.ValueOf(pair.MemoryValue))
				}
			}
		}).(*file)
	}

	// Create manifest
	m := New(content.Project.ID, content.Project.APIHost)
	m.allowTargetENV = content.AllowTargetENV
	m.mapping = mapping

	// Set configuration
	m.SetSortBy(content.SortBy)
	m.naming = content.Naming
	m.filter.SetAllowedBranches(content.AllowedBranches)
	m.filter.SetIgnoredComponents(content.IgnoredComponents)
	m.repositories = content.Templates.Repositories
	m.vaultVariables = content.Vault.Variables

	// Set records
	if err := m.SetRecords(content.records()); err != nil && !ignoreErrors {
		return nil, InvalidManifestError{errors.PrefixError(err, "invalid manifest")}
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(ctx context.Context, fs filesystem.Fs) error {
	// Create file content
	content := newFile(m.ProjectID(), m.APIHost())
	content.AllowTargetENV = m.allowTargetENV
	content.SortBy = m.SortBy()
	content.Naming = m.naming
	content.AllowedBranches = m.filter.AllowedBranches()
	content.IgnoredComponents = m.filter.IgnoredComponents()
	content.Templates.Repositories = m.repositories
	content.Vault.Variables = m.vaultVariables
	content.setRecords(m.All())

	// Map memory IDs to manifest IDs
	if len(m.mapping) > 0 {
		content = deepcopy.CopyTranslate(content, func(original, clone reflect.Value, path deepcopy.Path) {
			for _, pair := range m.mapping {
				if original.Interface() == pair.MemoryValue {
					clone.Set(reflect.ValueOf(pair.ManifestValue))
				}
			}
		}).(*file)
	}

	// Replace main branch in the filter, with branch ID, if needed
	if content.AllowTargetENV && len(content.AllowedBranches) == 1 && content.AllowedBranches[0] == model.MainBranchDef && len(content.Branches) == 1 {
		content.AllowedBranches[0] = model.AllowedBranch(content.Branches[0].ID.String())
	}

	// Save file
	if err := saveFile(ctx, fs, content); err != nil {
		return err
	}

	m.ResetChanged()
	return nil
}

func (m *Manifest) Path() string {
	return Path()
}

func (m *Manifest) Filter() *model.ObjectsFilter {
	return &m.filter
}

func (m *Manifest) APIHost() string {
	return m.project.APIHost
}

func (m *Manifest) ProjectID() keboola.ProjectID {
	return m.project.ID
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.naming
}

func (m *Manifest) SetNamingTemplate(v naming.Template) {
	m.naming = v
}

func (m *Manifest) AllowTargetENV() bool {
	return m.allowTargetENV
}

func (m *Manifest) SetAllowTargetENV(v bool) {
	m.allowTargetENV = v
}

func (m *Manifest) AllowedBranches() model.AllowedBranches {
	return m.filter.AllowedBranches()
}

func (m *Manifest) SetAllowedBranches(branches model.AllowedBranches) {
	m.filter.SetAllowedBranches(branches)
}

func (m *Manifest) IgnoredComponents() model.ComponentIDs {
	return m.filter.IgnoredComponents()
}

func (m *Manifest) SetIgnoredComponents(ids model.ComponentIDs) {
	m.filter.SetIgnoredComponents(ids)
}

func (m *Manifest) IsChanged() bool {
	return m.records.IsChanged()
}

func (m *Manifest) IsObjectIgnored(object model.Object) bool {
	return m.filter.IsObjectIgnored(object)
}

func (m *Manifest) TemplateRepository(name string) (model.TemplateRepository, bool) {
	for _, r := range m.repositories {
		if r.Name == name {
			return r, true
		}
	}
	return model.TemplateRepository{}, false
}

func (m *Manifest) VaultVariables() []*keboola.VaultVariable {
	return m.vaultVariables
}

func (m *Manifest) SetVaultVariables(variables []*keboola.VaultVariable) {
	m.vaultVariables = variables
}

func (m *Manifest) AddVaultVariable(variable *keboola.VaultVariable) {
	m.vaultVariables = append(m.vaultVariables, variable)
}

func (m *Manifest) RemoveVaultVariable(hash keboola.VaultVariableHash) bool {
	for i, v := range m.vaultVariables {
		if v.Hash == hash {
			m.vaultVariables = append(m.vaultVariables[:i], m.vaultVariables[i+1:]...)
			return true
		}
	}
	return false
}
