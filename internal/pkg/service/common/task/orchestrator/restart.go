package orchestrator

// RestartedByTimer is an error used by orchestrator to restart underlying watch stream periodically.
// It's a safeguard against if the stream stops working, and we don't detect it - which shouldn't happen.
type RestartedByTimer struct {
	error
}

// RestartedByDistribution is an error used by orchestrator to restart underlying watch stream on distribution change.
// If there is a change in the topology of the nodes (some node is added/removed),
// then the responsibilities/division of tasks will change, so it is necessary to restart the orchestrator.
type RestartedByDistribution struct {
	error
}

func (e RestartedByTimer) Unwrap() error {
	return e.error
}

func (e RestartedByDistribution) Unwrap() error {
	return e.error
}
