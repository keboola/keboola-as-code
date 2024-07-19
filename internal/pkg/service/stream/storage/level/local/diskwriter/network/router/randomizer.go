package router

import (
	"math/rand/v2"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Randomizer interface {
	// IntN - [0,n)
	IntN(n int) int
	// Float64 - [0.0,1.0)
	Float64() float64
}

type DefaultRandomizer struct {
	rand *rand.Rand
}

type TestRandomizer struct {
	queueIntN    []int
	queueFloat64 []float64
}

func NewDefaultRandomizer() Randomizer {
	return &DefaultRandomizer{rand: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 123))}
}

func NewTestRandomizer() *TestRandomizer {
	return &TestRandomizer{}
}

func (r *DefaultRandomizer) IntN(n int) int {
	return r.rand.IntN(n)
}

func (r *DefaultRandomizer) Float64() float64 {
	return r.rand.Float64()
}

func (r *TestRandomizer) QueueIntN(v int) {
	r.queueIntN = append(r.queueIntN, v)
}

func (r *TestRandomizer) QueueFloat64(v float64) {
	if v < 0 || v >= 1 {
		panic(errors.New("expected [0.0,1.0)"))
	}
	r.queueFloat64 = append(r.queueFloat64, v)
}

func (r *TestRandomizer) IntN(n int) int {
	if len(r.queueIntN) > 0 {
		v := r.queueIntN[0]
		r.queueIntN = r.queueIntN[1:]
		if v >= n {
			v = n - 1
		}
		return v
	}
	return 0
}

func (r *TestRandomizer) Float64() float64 {
	if len(r.queueFloat64) > 0 {
		v := r.queueFloat64[0]
		r.queueFloat64 = r.queueFloat64[1:]
		return v
	}
	return 0
}
