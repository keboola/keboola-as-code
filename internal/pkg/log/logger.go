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
	sugaredLogger *zap.SugaredLogger
	core          zapcore.Core
	component     string
}

func loggerFromZapCore(core zapcore.Core) *zapLogger {
	return &zapLogger{sugaredLogger: zap.New(core).Sugar(), core: core}
}

// With creates a child logger and adds structured context to it.
func (l *zapLogger) With(attrs ...attribute.KeyValue) Logger {
	var fields []zap.Field
	ctxattr.AttrsToZapFields(attrs, &fields)
	core := l.core.With(fields)
	clone := loggerFromZapCore(core)
	clone.component = l.component
	return clone
}

// WithComponent creates a child logger with added component.
func (l *zapLogger) WithComponent(component string) Logger {
	if l.component != "" {
		component = l.component + "." + component
	}
	clone := loggerFromZapCore(l.core)
	clone.component = component
	return clone
}

func formatMessageUsingAttributes(message string, set *attribute.Set) string {
	replacements := []string{}
	for _, keyValue := range set.ToSlice() {
		replacements = append(replacements, "<"+string(keyValue.Key)+">", keyValue.Value.Emit())
	}
	return strings.NewReplacer(replacements...).Replace(message)
}

func (l *zapLogger) prepareFields(ctx context.Context) []zap.Field {
	fields := ctxattr.ZapFields(ctx)
	if l.component != "" {
		fields = append(fields, zap.String(ComponentKey, l.component))
	}
	return fields
}

func (l *zapLogger) Debug(ctx context.Context, message string) {
	l.sugaredLogger.Desugar().Debug(
		formatMessageUsingAttributes(message, ctxattr.Attributes(ctx)),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Info(ctx context.Context, message string) {
	l.sugaredLogger.Desugar().Info(
		formatMessageUsingAttributes(message, ctxattr.Attributes(ctx)),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Warn(ctx context.Context, message string) {
	l.sugaredLogger.Desugar().Warn(
		formatMessageUsingAttributes(message, ctxattr.Attributes(ctx)),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Error(ctx context.Context, message string) {
	l.sugaredLogger.Desugar().Error(
		formatMessageUsingAttributes(message, ctxattr.Attributes(ctx)),
		l.prepareFields(ctx)...,
	)
}

func formatMessage(args ...any) string {
	if len(args) == 1 {
		if str, ok := args[0].(string); ok {
			return str
		}
	}
	return fmt.Sprint(args...)
}

func (l *zapLogger) Log(ctx context.Context, level string, args ...any) {
	message := formatMessage(args...)
	fields := l.prepareFields(ctx)
	switch level {
	case "debug", "DEBUG":
		l.sugaredLogger.Desugar().Debug(message, fields...)
	case "info", "INFO":
		l.sugaredLogger.Desugar().Info(message, fields...)
	case "warn", "WARN":
		l.sugaredLogger.Desugar().Warn(message, fields...)
	case "error", "ERROR":
		l.sugaredLogger.Desugar().Error(message, fields...)
	case "dpanic", "DPANIC":
		l.sugaredLogger.Desugar().DPanic(message, fields...)
	case "panic", "PANIC":
		l.sugaredLogger.Desugar().Panic(message, fields...)
	case "fatal", "FATAL":
		l.sugaredLogger.Desugar().Fatal(message, fields...)
	default:
		l.sugaredLogger.Desugar().Info(message, fields...)
	}
}

func (l *zapLogger) Debugf(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Debug(
		fmt.Sprintf(formatMessageUsingAttributes(template, ctxattr.Attributes(ctx)), args...),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Infof(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Info(
		fmt.Sprintf(formatMessageUsingAttributes(template, ctxattr.Attributes(ctx)), args...),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Warnf(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Warn(
		fmt.Sprintf(formatMessageUsingAttributes(template, ctxattr.Attributes(ctx)), args...),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Errorf(ctx context.Context, template string, args ...any) {
	l.sugaredLogger.Desugar().Error(
		fmt.Sprintf(formatMessageUsingAttributes(template, ctxattr.Attributes(ctx)), args...),
		l.prepareFields(ctx)...,
	)
}

func (l *zapLogger) Sync() error {
	return l.sugaredLogger.Sync()
}

func (l *zapLogger) zapCore() zapcore.Core {
	return l.core
}
