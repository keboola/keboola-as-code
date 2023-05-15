package httpserver

type Config struct {
	ListenAddress     string
	ErrorNamePrefix   string
	ExceptionIDPrefix string
	// Mount endpoints to the Muxer
	Mount func(c Components)
}
