// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

const ComponentKey = "component"

// zapLogger is default implementation of the Logger interface.
// It is wrapped zap.SugaredLogger.
type zapLogger struct {
	logger     *zap.Logger
	core       zapcore.Core
	attributes []attribute.KeyValue
	component  string
}

func newLoggerFromZapCore(core zapcore.Core) *zapLogger {
	return &zapLogger{logger: zap.New(core), core: core}
}

// With creates a child logger and adds structured context to it.
func (l *zapLogger) With(attrs ...attribute.KeyValue) Logger {
	var fields []zap.Field
	ctxattr.AttrsToZapFields(attrs, &fields)

	core := l.core.With(fields)

	clone := newLoggerFromZapCore(core)
	clone.attributes = append(clone.attributes, l.attributes...)
	clone.attributes = append(clone.attributes, attrs...)
	clone.component = l.component

	return clone
}

// WithComponent creates a child logger with added component.
func (l *zapLogger) WithComponent(component string) Logger {
	if l.component != "" {
		component = l.component + "." + component
	}
	clone := *l
	clone.component = component
	return &clone
}

func (l *zapLogger) WithDuration(v time.Duration) Logger {
	return l.With(attribute.String("duration", v.String()))
}

func (l *zapLogger) Debug(ctx context.Context, message string) {
	if l.core.Enabled(DebugLevel) {
		l.logger.Debug(l.message(ctx, message), l.fields(ctx)...)
	}
}

func (l *zapLogger) Info(ctx context.Context, message string) {
	if l.core.Enabled(InfoLevel) {
		l.logger.Info(l.message(ctx, message), l.fields(ctx)...)
	}
}

func (l *zapLogger) Warn(ctx context.Context, message string) {
	if l.core.Enabled(WarnLevel) {
		l.logger.Warn(l.message(ctx, message), l.fields(ctx)...)
	}
}

func (l *zapLogger) Error(ctx context.Context, message string) {
	if l.core.Enabled(ErrorLevel) {
		l.logger.Error(l.message(ctx, message), l.fields(ctx)...)
	}
}

func (l *zapLogger) Log(ctx context.Context, level, message string) {
	l.logInLevel(level, l.message(ctx, message), l.fields(ctx)...)
}

func (l *zapLogger) Debugf(ctx context.Context, template string, args ...any) {
	if l.core.Enabled(DebugLevel) {
		l.logger.Debug(l.template(ctx, template, args), l.fields(ctx)...)
	}
}

func (l *zapLogger) Infof(ctx context.Context, template string, args ...any) {
	if l.core.Enabled(InfoLevel) {
		l.logger.Info(l.template(ctx, template, args), l.fields(ctx)...)
	}
}

func (l *zapLogger) Warnf(ctx context.Context, template string, args ...any) {
	if l.core.Enabled(WarnLevel) {
		l.logger.Warn(l.template(ctx, template, args), l.fields(ctx)...)
	}
}

func (l *zapLogger) Errorf(ctx context.Context, template string, args ...any) {
	if l.core.Enabled(ErrorLevel) {
		l.logger.Error(l.template(ctx, template, args), l.fields(ctx)...)
	}
}

func (l *zapLogger) Logf(ctx context.Context, level, template string, args ...any) {
	l.logInLevel(level, l.template(ctx, template, args), l.fields(ctx)...)
}

func (l *zapLogger) Sync() error {
	return l.logger.Sync()
}

func (l *zapLogger) ZapCore() zapcore.Core {
	return l.core
}

func (l *zapLogger) logInLevel(level string, message string, fields ...zap.Field) {
	switch level {
	case "debug", "DEBUG":
		if l.core.Enabled(DebugLevel) {
			l.logger.Debug(message, fields...)
		}
	case "info", "INFO":
		if l.core.Enabled(InfoLevel) {
			l.logger.Info(message, fields...)
		}
	case "warn", "WARN":
		if l.core.Enabled(WarnLevel) {
			l.logger.Warn(message, fields...)
		}
	case "error", "ERROR":
		if l.core.Enabled(ErrorLevel) {
			l.logger.Error(message, fields...)
		}
	case "dpanic", "DPANIC":
		l.logger.DPanic(message, fields...)
	case "panic", "PANIC":
		l.logger.Panic(message, fields...)
	case "fatal", "FATAL":
		l.logger.Fatal(message, fields...)
	default:
		if l.core.Enabled(InfoLevel) {
			l.logger.Info(message, fields...)
		}
	}
}

func (l *zapLogger) message(ctx context.Context, message string) string {
	var replacements []string
	for _, kv := range ctxattr.Attributes(ctx).ToSlice() {
		replacements = append(replacements, "<"+string(kv.Key)+">", kv.Value.Emit())
	}
	for _, kv := range l.attributes {
		replacements = append(replacements, "<"+string(kv.Key)+">", kv.Value.Emit())
	}
	return strings.NewReplacer(replacements...).Replace(message)
}

func (l *zapLogger) template(ctx context.Context, template string, args []any) string {
	return fmt.Sprintf(l.message(ctx, template), args...)
}

func (l *zapLogger) fields(ctx context.Context) []zap.Field {
	fields := ctxattr.ZapFields(ctx)
	if l.component != "" {
		fields = append(fields, zap.String(ComponentKey, l.component))
	}
	return fields
}
