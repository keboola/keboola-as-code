package source

type Config struct {
	HTTP HTTPSourceConfig `configKey:"http"`
}

type HTTPSourceConfig struct {
	Listen string `configKey:"listen" configUsage:"Listen address of the HTTP source."  validate:"required,hostname_port"`
}
