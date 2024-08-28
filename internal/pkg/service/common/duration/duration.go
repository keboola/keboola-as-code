// Package duration provides a wrapper for time.Duration type,
// to serialize duration as string instead of int64 nanoseconds.
//
// This cannot be changed in Go 1.X, it would break backward compatibility, see:
// https://github.com/golang/go/issues/10275
// "This isn't possible to change now, as it would change the encodings produced by programs that exist today."
package duration

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func From(duration time.Duration) Duration {
	return Duration(duration)
}

func (v Duration) Duration() time.Duration {
	return time.Duration(v)
}

func (v Duration) String() string {
	return v.Duration().String()
}

func (v Duration) MarshalText() (text []byte, err error) {
	return []byte(v.String()), nil
}

func (v *Duration) UnmarshalText(text []byte) error {
	duration, err := time.ParseDuration(string(text))
	*v = Duration(duration)
	return err
}

func (v *Duration) UnmarshalJSON(b []byte) error {
	// String, for example, "1h20s"
	if bytes.HasPrefix(b, []byte(`"`)) && bytes.HasSuffix(b, []byte(`"`)) {
		return v.UnmarshalText(bytes.Trim(b, `"`))
	}
	// Nanoseconds int64 - backward compatibility
	return json.Unmarshal(b, (*time.Duration)(v))
}

func (v *Duration) UnmarshalYAML(n *yaml.Node) error {
	// String, for example, "1h20s"
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		return v.UnmarshalText([]byte(strings.Trim(n.Value, `"`)))
	}
	// Nanoseconds int64 - backward compatibility
	return n.Decode((*int64)(v))
}
