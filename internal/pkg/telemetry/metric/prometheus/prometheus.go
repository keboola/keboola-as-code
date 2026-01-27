// Package prometheus provides HTTP metrics endpoint with OpenTelemetry metrics for Prometheus scraper.
package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/bridge/opencensus"
	export "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

const (
	Endpoint        = "metrics"
	startTimeout    = 10 * time.Second
	shutdownTimeout = 30 * time.Second
)

type Config struct {
	Listen string `configKey:"listen" configUsage:"Prometheus scraping metrics listen address." validate:"required,hostname_port"`
}

type errLogger struct {
	logger log.Logger
}

func (l *errLogger) Println(v ...any) {
	// The prometheus library doesn't provide a context of the message, so we have no choice but to use context.Background().
	l.logger.Error(context.Background(), fmt.Sprint(v...))
}

func NewConfig() Config {
	return Config{
		Listen: "0.0.0.0:9000",
	}
}

// ServeMetrics starts HTTP server for Prometheus metrics and return OpenTelemetry metrics provider.
// Inspired by: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/prometheus/main.go
func ServeMetrics(ctx context.Context, cfg Config, logger log.Logger, proc *servicectx.Process, serviceName string) (*metric.MeterProvider, error) {
	logger = logger.WithComponent("metrics")
	logger.Infof(ctx, `starting HTTP server on %q`, cfg.Listen)

	// Create resource
	res, err := resource.New(
		ctx,
		resource.WithAttributes(attribute.String("service_name", serviceName)),
		// resource.WithFromEnv(), // unused
		// resource.WithTelemetrySDK(), // unused
	)
	if err != nil {
		return nil, err
	}

	// Create metrics registry and exporter
	registry := prometheus.NewRegistry()
	exporter, err := export.New(export.WithRegisterer(registry), export.WithoutScopeInfo())
	if err != nil {
		return nil, err
	}

	// Create HTTP metrics server
	opts := promhttp.HandlerOpts{ErrorLog: &errLogger{logger: logger}}
	handler := http.NewServeMux()
	handler.Handle("/"+Endpoint, promhttp.HandlerFor(registry, opts))
	srv := &http.Server{Addr: cfg.Listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	proc.Add(func(shutdown servicectx.ShutdownFn) {
		logger.Infof(ctx, `started HTTP server on %q, endpoint %q"`, cfg.Listen, Endpoint)
		serverErr := srv.ListenAndServe()         // ListenAndServe blocks while the server is running
		shutdown(context.Background(), serverErr) // nolint: contextcheck // intentionally creating new context for the shutdown operation
	})
	proc.OnShutdown(func(ctx context.Context) {
		// Shutdown gracefully with a 30s timeout.
		ctx, cancel := context.WithTimeoutCause(ctx, shutdownTimeout, errors.New("shutdown timeout"))
		defer cancel()

		logger.Infof(ctx, `shutting down HTTP server at %q`, cfg.Listen)

		if err := srv.Shutdown(ctx); err != nil {
			logger.Errorf(ctx, `HTTP server shutdown error: %s`, err)
		}
		logger.Info(ctx, "HTTP server shutdown finished")
	})

	// Wait for server
	if err := netutils.WaitForTCP(srv.Addr, startTimeout); err != nil {
		return nil, errors.Errorf(`metrics server did not start: %w`, err)
	}

	// Create OpenTelemetry metrics provider
	provider := metric.NewMeterProvider(
		metric.WithReader(exporter),
		// Register legacy OpenCensus metrics, for go-cloud (https://github.com/google/go-cloud/issues/2877)
		metric.WithReader(metric.NewManualReader(metric.WithProducer(opencensus.NewMetricProducer()))),
		metric.WithResource(res),
		metric.WithView(View()),
	)
	return provider, nil
}

func View() metric.View {
	return metric.NewView(
		metric.Instrument{Name: "*"},
		metric.Stream{AttributeFilter: func(value attribute.KeyValue) bool {
			switch value.Key {
			// Remove unused attributes.
			case "http.flavor", "net.protocol.name", "net.protocol.version":
				return false
			}
			return true
		}},
	)
}
