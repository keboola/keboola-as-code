package manifest

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type TemplateRecord struct {
	ID           string          `json:"id" validate:"required,alphanumdash,min=1,max=40"`
	Name         string          `json:"name" validate:"required,min=1,max=40"`
	Description  string          `json:"description" validate:"required,min=1,max=200"`
	Requirements Requirements    `json:"requirements"`
	Categories   []string        `json:"categories,omitempty"`
	Deprecated   bool            `json:"deprecated,omitempty"`
	Path         string          `json:"path,omitempty"`
	Versions     []VersionRecord `json:"versions" validate:"required,min=1,dive"`
}

type VersionRecord struct {
	Version     model.SemVersion `json:"version" validate:"required,semver,min=1,max=20"`
	Description string           `json:"description" validate:"min=0,max=40"`
	Stable      bool             `json:"stable" validate:""`
	Components  []string         `json:"components,omitempty"`
	Path        string           `json:"path,omitempty"`
}

type Requirements struct {
	Backends   []string `json:"backends"`
	Components []string `json:"components"`
	Features   []string `json:"features"`
}

func (v *TemplateRecord) AllVersions() (out []VersionRecord) {
	// No version?
	if v.Versions == nil {
		return nil
	}

	// Sort the latest version first
	out = make([]VersionRecord, len(v.Versions))
	copy(out, v.Versions)
	sort.SliceStable(out, func(i, j int) bool {
		return out[j].Version.Value().LessThan(out[i].Version.Value())
	})
	return out
}

func (v *TemplateRecord) AddVersion(version model.SemVersion, components []string) VersionRecord {
	record := VersionRecord{
		Version:    version,
		Stable:     false,
		Components: components,
		Path:       fmt.Sprintf(`v%d`, version.Major()),
	}
	v.Versions = append(v.Versions, record)
	return record
}

// GetVersion returns template version record for wanted version.
// Wanted version doesn't have to contain the minor/path part.
// Example:
// "v1"     -> "1.2.3"
// "v1.1"   -> "1.1.1"
// "v1.1.0" -> "1.1.0".
func (v *TemplateRecord) GetVersion(wanted model.SemVersion) (VersionRecord, bool) {
	dotsCount := len(strings.Split(wanted.Original(), "."))
	minorIsSet := dotsCount >= 2
	patchIsSet := dotsCount >= 3

	// Iterate from the latest version.
	for _, version := range v.AllVersions() {
		value := version.Version
		found := value.Major() == wanted.Major() &&
			(value.Minor() == wanted.Minor() || !minorIsSet) &&
			(value.Patch() == wanted.Patch() || !patchIsSet)
		if found {
			return version, true
		}
	}
	return VersionRecord{}, false
}

func (v *TemplateRecord) GetVersionOrErr(wantedStr string) (VersionRecord, error) {
	// Parse version
	var wanted model.SemVersion
	if wantedStr == "" {
		if v, err := v.DefaultVersionOrErr(); err != nil {
			return VersionRecord{}, err
		} else {
			wanted = v.Version
		}
	} else {
		if v, err := model.NewSemVersion(wantedStr); err != nil {
			return VersionRecord{}, err
		} else {
			wanted = v
		}
	}

	// Get version
	version, found := v.GetVersion(wanted)
	if !found {
		return version, VersionNotFoundError{errors.Errorf(`template "%s" found but version "%s" is missing`, v.ID, wanted.Original())}
	}
	return version, nil
}

func (v *TemplateRecord) GetClosestVersion(wanted model.SemVersion) (VersionRecord, bool) {
	if version, found := v.GetVersion(wanted); found {
		return version, true
	}
	if version, found := v.GetVersion(wanted.ToMinor()); found {
		return version, true
	}

	if version, found := v.GetVersion(wanted.ToMajor()); found {
		return version, true
	}
	return v.DefaultVersion()
}

func (v *TemplateRecord) GetByPath(path string) (VersionRecord, bool) {
	for _, record := range v.Versions {
		if record.Path == path {
			return record, true
		}
	}
	return VersionRecord{}, false
}

func (v *TemplateRecord) DefaultVersion() (VersionRecord, bool) {
	found := false
	latest := VersionRecord{Version: model.ZeroSemVersion()}
	latestStable := VersionRecord{Version: model.ZeroSemVersion()}
	for _, item := range v.AllVersions() {
		// GreaterThanOrEqual
		if !item.Version.LessThan(latest.Version.Value()) {
			found = true
			latest = item
			if item.Stable {
				latestStable = item
			}
		}
	}

	// Stable version found
	if latestStable.Version.GreaterThan(model.ZeroSemVersion().Value()) {
		return latestStable, found
	}

	// No stable version found
	return latest, found
}

func (v *TemplateRecord) DefaultVersionOrErr() (VersionRecord, error) {
	version, found := v.DefaultVersion()
	if !found {
		return version, VersionNotFoundError{errors.Errorf(`default version for template "%s" was not found`, v.ID)}
	}
	return version, nil
}

// CheckProjectComponents - all required components must be present.
func (v *TemplateRecord) CheckProjectComponents(components *model.ComponentsMap) bool {
	for _, component := range v.Requirements.Components {
		if _, found := components.Get(keboola.ComponentID(component)); !found {
			return false
		}
	}
	return true
}

// CheckProjectFeatures - all required project features must be present.
func (v *TemplateRecord) CheckProjectFeatures(d keboola.FeaturesMap) bool {
	for _, feature := range v.Requirements.Features {
		if !d.Has(feature) {
			return false
		}
	}
	return true
}

// HasBackend - at least one required backend must be present.
func (v *TemplateRecord) HasBackend(projectBackends []string) bool {
	if len(v.Requirements.Backends) == 0 {
		return true
	}
	for _, backend := range v.Requirements.Backends {
		if slices.Contains(projectBackends, backend) {
			return true
		}
	}
	return false
}
