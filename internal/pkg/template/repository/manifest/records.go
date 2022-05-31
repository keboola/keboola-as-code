package manifest

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type TemplateRecord struct {
	Id            string `json:"id" validate:"required,alphanumdash,min=1,max=40"`
	Name          string `json:"name" validate:"required,min=1,max=40"`
	Description   string `json:"description" validate:"required,min=1,max=200"`
	model.AbsPath `validate:"dive"`
	Versions      []VersionRecord `json:"versions" validate:"required,min=1,dive"`
}

type VersionRecord struct {
	Version       model.SemVersion `json:"version" validate:"required,semver,min=1,max=20"`
	Description   string           `json:"description" validate:"min=0,max=40"`
	Stable        bool             `json:"stable" validate:""`
	Components    []string         `json:"components,omitempty"`
	model.AbsPath `validate:"dive"`
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
		AbsPath:    model.NewAbsPath(v.Path(), fmt.Sprintf(`v%d`, version.Major())),
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
		return version, VersionNotFoundError{fmt.Errorf(`template "%s" found but version "%s" is missing`, v.Id, wanted.Original())}
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
		if record.GetRelativePath() == path {
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
		return version, VersionNotFoundError{fmt.Errorf(`default version for template "%s" was not found`, v.Id)}
	}
	return version, nil
}
