package model

import (
	"fmt"

	"github.com/Masterminds/semver/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type value = semver.Version

// SemVersion is wrapper around semver.Version - for better error message in UnmarshalJSON.
type SemVersion struct {
	*value
}

func NewSemVersion(str string) (SemVersion, error) {
	v, err := semver.NewVersion(str)
	if err != nil {
		return SemVersion{}, err
	}
	return SemVersion{value: v}, nil
}

func ZeroSemVersion() SemVersion {
	v, err := NewSemVersion(`0.0.1`)
	if err != nil {
		panic(err)
	}
	return v
}

func (v SemVersion) Original() string {
	return v.value.Original()
}

func (v SemVersion) Value() *semver.Version {
	value := *v.value
	return &value
}

// IncMajor increments major version, for example 1.2.3 -> 2.0.0.
func (v SemVersion) IncMajor() SemVersion {
	newVersion := v.Value().IncMajor()
	return SemVersion{value: &newVersion}
}

func (v SemVersion) ToMinor() SemVersion {
	out, err := NewSemVersion(fmt.Sprintf(`v%d.%d`, v.Major(), v.Minor()))
	if err != nil {
		panic(err)
	}
	return out
}

func (v SemVersion) ToMajor() SemVersion {
	out, err := NewSemVersion(fmt.Sprintf(`v%d`, v.Major()))
	if err != nil {
		panic(err)
	}
	return out
}

// UnmarshalJSON returns human-readable error message, if semantic version is invalid.
func (v *SemVersion) UnmarshalJSON(b []byte) (err error) {
	var versionStr string
	if err := json.Decode(b, &versionStr); err != nil {
		return err
	}

	value, err := semver.NewVersion(versionStr)
	if err != nil {
		return errors.Errorf(`invalid semantic version "%s"`, versionStr)
	}
	v.value = value
	return nil
}
