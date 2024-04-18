package testutil

import "go.opentelemetry.io/otel/attribute"

func ExpectedServerReqAttrs(host string, status int, appID, appName, appUpstream string) attribute.Set {
	attrs := []attribute.KeyValue{
		attribute.String("http.method", "GET"),
		attribute.String("http.scheme", "https"),
	}

	if status != 0 {
		attrs = append(attrs, attribute.Int("http.status_code", 200))
	}

	if host != "" {
		attrs = append(attrs, attribute.String("net.host.name", host))
	}

	if appID != "" {
		attrs = append(attrs, attribute.String("proxy.app.id", "123"))
	}

	if appName != "" {
		attrs = append(attrs, attribute.String("proxy.app.name", "public"))
	}

	if appUpstream != "" {
		attrs = append(attrs, attribute.String("proxy.app.upstream", appUpstream))
	}

	return attribute.NewSet(attrs...)
}
