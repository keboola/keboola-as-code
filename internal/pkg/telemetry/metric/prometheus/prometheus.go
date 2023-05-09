// Package prometheus provides HTTP metrics endpoint with OpenTelemetry metrics for Prometheus scraper.
package prometheus

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
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

type errLogger struct {
	logger log.Logger
}

func (l *errLogger) Println(v ...any) {
	l.logger.Error(v...)
}

// ServeMetrics starts HTTP server for Prometheus metrics and return OpenTelemetry metrics provider.
// Inspired by: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/prometheus/main.go
func ServeMetrics(ctx context.Context, serviceName, listenAddr string, logger log.Logger, proc *servicectx.Process) (*metric.MeterProvider, error) {
	logger = logger.AddPrefix("[metrics]")

	// Create resource
	res, err := resource.New(
		ctx,
		resource.WithAttributes(attribute.String("service_name", serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, err
	}

	// Create metrics registry and exporter
	registry := prometheus.NewRegistry()
	exporter, err := export.New(export.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}

	// Create HTTP metrics server
	opts := promhttp.HandlerOpts{ErrorLog: &errLogger{logger: logger}}
	handler := http.NewServeMux()
	handler.Handle("/"+Endpoint, promhttp.HandlerFor(registry, opts))
	srv := &http.Server{Addr: listenAddr, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	proc.Add(func(ctx context.Context, shutdown servicectx.ShutdownFn) {
		logger.Infof(`HTTP server listening on "%s/%s"`, listenAddr, Endpoint)
		shutdown(srv.ListenAndServe())
	})
	proc.OnShutdown(func() {
		logger.Infof(`shutting down HTTP server at "%s"`, listenAddr)

		// Shutdown gracefully with a 30s timeout.
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			logger.Errorf(`HTTP server shutdown error: %s`, err)
		}
		logger.Info("HTTP server shutdown finished")
	})

	// Wait for server
	if err := netutils.WaitForTCP(srv.Addr, startTimeout); err != nil {
		return nil, errors.Errorf(`metrics server did not start: %w`, err)
	}

	// Create OpenTelemetry metrics provider
	provider := metric.NewMeterProvider(metric.WithReader(exporter), metric.WithResource(res))
	return provider, nil
}
