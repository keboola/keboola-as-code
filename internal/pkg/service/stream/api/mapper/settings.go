package mapper

import (
	"strings"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Mapper) NewSettingsPatch(apiPatch api.SettingsPatch, superAdmin bool) (patchKVs configpatch.PatchKVs, err error) {
	// Map
	for _, kv := range apiPatch {
		patchKVs = append(patchKVs, configpatch.PatchKV{
			KeyPath: kv.Key,
			Value:   kv.Value,
		})
	}

	// Validation options
	var opts []configpatch.Option
	if superAdmin {
		opts = append(opts, configpatch.WithModifyProtected())
	}

	// Validation
	cfg := deepcopy.Copy(m.config).(config.Config)
	patch := config.Patch{}
	if err := configpatch.ApplyKVs(&cfg, &patch, patchKVs, opts...); err != nil {
		if errors.As(err, &configpatch.ProtectedKeyError{}) {
			return nil, svcerrors.NewForbiddenError(err)
		} else {
			return nil, svcerrors.NewBadRequestError(err)
		}
	}

	return patchKVs, nil
}

func (m *Mapper) NewSettingsResponse(patchKVs configpatch.PatchKVs) (*api.SettingsResult, error) {
	// Convert patch KVs to the patch structure
	patch := config.Patch{}
	if err := configpatch.BindKVs(&patch, patchKVs); err != nil {
		return nil, err
	}

	// Dump
	kvs, err := configpatch.DumpAll(m.config, patch)
	if err != nil {
		return nil, err
	}

	out := &api.SettingsResult{}
	for _, kv := range kvs {
		// Simplify types
		tp := kv.Type
		if strings.Contains(tp, "int") {
			tp = "int"
		}
		if strings.Contains(tp, "float") {
			tp = "float"
		}

		mapped := &api.SettingResult{
			Key:          kv.KeyPath,
			Type:         tp,
			Description:  kv.Description,
			Value:        kv.Value,
			DefaultValue: kv.DefaultValue,
			Overwritten:  kv.Overwritten,
			Protected:    kv.Protected,
		}

		if kv.Validation != "" {
			validation := kv.Validation
			mapped.Validation = &validation
		}

		out.Settings = append(out.Settings, mapped)
	}

	return out, nil
}
