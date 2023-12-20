// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// noFieldsEncoder removes zap fields from output.
type noFieldsEncoder struct {
	zapcore.Encoder
}

func newNoFieldsEncoder(encoder zapcore.Encoder) zapcore.Encoder {
	return &noFieldsEncoder{Encoder: encoder}
}

func (v *noFieldsEncoder) Clone() zapcore.Encoder {
	clone := *v
	return &clone
}

func (v *noFieldsEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	return v.Encoder.EncodeEntry(entry, nil)
}
