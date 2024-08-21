package datadog

import "gopkg.in/DataDog/dd-trace-go.v1/profiler"

type Config struct {
	Enabled  bool           `configKey:"enabled" configUsage:"Enable DataDog integration."`
	Debug    bool           `configKey:"debug" configUsage:"Enable DataDog debug messages."`
	Profiler ProfilerConfig `configKey:"profiler"`
}

type ProfilerConfig struct {
	Enabled          bool `configKey:"enabled" configUsage:"Enable DataDog profiler. Don't use in the production."`
	CPUProfile       bool `configKey:"cpu" configUsage:"Enable CPU profile."`
	MemoryProfile    bool `configKey:"memory" configUsage:"Enable memory profile."`
	BlockProfile     bool `configKey:"block" configUsage:"Enable block profile, may have big overhead."`
	MutexProfile     bool `configKey:"mutex" configUsage:"Enable mutex profile, may have big overhead."`
	GoroutineProfile bool `configKey:"goroutine" configUsage:"Enable Goroutine profile, may have big overhead."`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
		Debug:   false,
		Profiler: ProfilerConfig{
			Enabled:          false,
			CPUProfile:       true,
			MemoryProfile:    true,
			BlockProfile:     false,
			MutexProfile:     false,
			GoroutineProfile: false,
		},
	}
}

func (c ProfilerConfig) ProfilerTypes() (out []profiler.ProfileType) {
	if c.CPUProfile {
		out = append(out, profiler.CPUProfile)
	}
	if c.MemoryProfile {
		out = append(out, profiler.HeapProfile)
	}
	if c.BlockProfile {
		out = append(out, profiler.BlockProfile)
	}
	if c.MutexProfile {
		out = append(out, profiler.MutexProfile)
	}
	if c.GoroutineProfile {
		out = append(out, profiler.GoroutineProfile)
	}
	return out
}
