package manifest

import (
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

func (v *TemplateRecord) LatestVersion() (latest VersionRecord, found bool) {
	zeroVersion, err := template.NewVersion(`0.0.0`)
	if err != nil {
		panic(err)
	}

	latest = VersionRecord{Version: zeroVersion}
	for _, item := range v.Versions {
		if item.Version.GreaterThan(latest.Version.Value()) {
			latest = item
			found = true
		}
	}
	return
}
