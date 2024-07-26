package etcdop

// RestartableWatchStreamT is restarted on a fatal error, or manually by the Restart method.
type RestartableWatchStreamT[T any] struct {
	*WatchStreamT[T]
	rawStream *RestartableWatchStreamRaw
}

// Restart cancels the current stream, so a new stream is created.
func (s *RestartableWatchStreamT[T]) Restart(cause error) {
	s.rawStream.Restart(cause)
}

func (s *RestartableWatchStreamT[T]) SetupConsumer() WatchConsumerSetup[T] {
	return newConsumerSetup[T](s)
}
