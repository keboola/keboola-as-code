package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type TemplateRecord struct {
	Id            string `json:"id" validate:"required,alphanumdash"`
	Name          string `json:"name" validate:"required"`
	Description   string `json:"description" validate:"required"`
	model.AbsPath `validate:"dive"`
	Versions      []VersionRecord `json:"versions" validate:"required,dive"`
}

type VersionRecord struct {
	Version       template.Version `json:"version" validate:"required,semver"`
	Description   string           `json:"description" validate:"required"`
	Stable        bool             `json:"stable" validate:"required"`
	model.AbsPath `validate:"dive"`
}

func (v *TemplateRecord) AddVersion(version template.Version) VersionRecord {
	record := VersionRecord{
		Version: version,
		Stable:  false,
		AbsPath: model.NewAbsPath(v.Path(), fmt.Sprintf(`v%d`, version.Major())),
	}
	v.Versions = append(v.Versions, record)
	return record
}

func (v *TemplateRecord) GetByPath(path string) (VersionRecord, bool) {
	for _, record := range v.Versions {
		if record.RelativePath == path {
			return record, true
		}
	}
	return VersionRecord{}, false
}

func (v *TemplateRecord) LatestVersion() (latest VersionRecord, found bool) {
	latest = VersionRecord{Version: template.ZeroVersion()}
	for _, item := range v.Versions {
		if item.Version.GreaterThan(latest.Version.Value()) {
			latest = item
			found = true
		}
	}
	return
}
