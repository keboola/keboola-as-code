// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const PrefixKey = "prefix"

// prefixEncoder adds a prefix from the context fields to the beginning of the encoded message.
type prefixEncoder struct {
	zapcore.Encoder
	prefix     string
	bufferPool buffer.Pool
}

func newPrefixEncoder(encoder zapcore.Encoder) zapcore.Encoder {
	return &prefixEncoder{Encoder: encoder, bufferPool: buffer.NewPool()}
}

func (v *prefixEncoder) AddString(key, value string) {
	// Catch the prefix context key
	if key == PrefixKey {
		v.prefix = value
	} else {
		v.Encoder.AddString(key, value)
	}
}

func (v *prefixEncoder) Clone() zapcore.Encoder {
	clone := *v
	return &clone
}

func (v *prefixEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	// Write prefix
	b := v.bufferPool.Get()
	if v.prefix != "" {
		_, _ = b.WriteString(v.prefix)
	}

	// Write original message
	originalBuffer, err := v.Encoder.EncodeEntry(entry, fields)
	if err != nil {
		return nil, err
	}
	_, _ = b.Write(originalBuffer.Bytes())
	originalBuffer.Free()

	return b, nil
}
