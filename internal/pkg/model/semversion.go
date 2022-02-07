package model

import (
	"fmt"

	"github.com/Masterminds/semver"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

type value = semver.Version

// SemVersion is wrapper around semver.Version - for better error message in UnmarshalJSON.
type SemVersion struct {
	value
}

func NewSemVersion(str string) (SemVersion, error) {
	v, err := semver.NewVersion(str)
	if err != nil {
		return SemVersion{}, err
	}
	return SemVersion{value: *v}, nil
}

func ZeroSemVersion() SemVersion {
	v, err := NewSemVersion(`0.0.1`)
	if err != nil {
		panic(err)
	}
	return v
}

func (v SemVersion) Value() *semver.Version {
	value := v.value
	return &value
}

// IncMajor increments major version, for example 1.2.3 -> 2.0.0.
func (v SemVersion) IncMajor() SemVersion {
	return SemVersion{value: v.Value().IncMajor()}
}

// UnmarshalJSON returns human-readable error message, if semantic version is invalid.
func (v *SemVersion) UnmarshalJSON(b []byte) (err error) {
	var versionStr string
	if err := json.Decode(b, &versionStr); err != nil {
		return err
	}

	value, err := semver.NewVersion(versionStr)
	if err != nil {
		return fmt.Errorf(`invalid semantic version "%s"`, versionStr)
	}
	v.value = *value
	return nil
}
