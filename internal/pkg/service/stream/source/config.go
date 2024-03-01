package source

import "net/url"

type Config struct {
	HTTP HTTPSourceConfig `configKey:"http"`
}

type ConfigPatch struct{}

type HTTPSourceConfig struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the HTTP source."  validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicUrl" configUsage:"Public URL of the HTTP source for link generation."  validate:"required"`
}

func NewConfig() Config {
	return Config{
		HTTP: HTTPSourceConfig{
			Listen:    "0.0.0.0:7000",
			PublicURL: nil,
		},
	}
}
