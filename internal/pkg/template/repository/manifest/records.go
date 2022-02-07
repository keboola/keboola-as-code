package manifest

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type TemplateRecord struct {
	Id            string `json:"id" validate:"required,alphanumdash"`
	Name          string `json:"name" validate:"required"`
	Description   string `json:"description" validate:"required"`
	model.AbsPath `validate:"dive"`
	Versions      []VersionRecord `json:"versions" validate:"required,dive"`
}

type VersionRecord struct {
	Version       model.SemVersion `json:"version" validate:"required,semver"`
	Description   string           `json:"description" validate:"required"`
	Stable        bool             `json:"stable" validate:"required"`
	model.AbsPath `validate:"dive"`
}

func (v *TemplateRecord) AddVersion(version model.SemVersion) VersionRecord {
	record := VersionRecord{
		Version: version,
		Stable:  false,
		AbsPath: model.NewAbsPath(v.Path(), fmt.Sprintf(`v%d`, version.Major())),
	}
	v.Versions = append(v.Versions, record)
	return record
}

// GetByVersion returns template version record for wanted version.
// Wanted version doesn't have to contain the minor/path part.
// Example:
// "v1"     -> "1.2.3"
// "v1.1"   -> "1.1.1"
// "v1.1.0" -> "1.1.0".
func (v *TemplateRecord) GetByVersion(wanted model.SemVersion) (VersionRecord, bool) {
	dotsCount := len(strings.Split(wanted.Original(), "."))
	minorIsSet := dotsCount >= 2
	patchIsSet := dotsCount >= 3

	// Latest version first
	reversedVersions := make([]VersionRecord, len(v.Versions))
	copy(reversedVersions, v.Versions)
	sort.SliceStable(reversedVersions, func(i, j int) bool {
		return reversedVersions[j].Version.Value().LessThan(reversedVersions[i].Version.Value())
	})

	// Iterate from the latest version.
	for _, version := range reversedVersions {
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

func (v *TemplateRecord) GetByPath(path string) (VersionRecord, bool) {
	for _, record := range v.Versions {
		if record.GetRelativePath() == path {
			return record, true
		}
	}
	return VersionRecord{}, false
}

func (v *TemplateRecord) LatestVersion() (latest VersionRecord, found bool) {
	latest = VersionRecord{Version: model.ZeroSemVersion()}
	for _, item := range v.Versions {
		if item.Version.GreaterThan(latest.Version.Value()) {
			latest = item
			found = true
		}
	}
	return
}
