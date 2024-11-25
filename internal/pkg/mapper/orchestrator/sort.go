package orchestrator

import (
	"sort"
	"strings"

	"v.io/x/lib/toposort"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type phasesSorter struct {
	phasesKeys         []string                // all keys in the order as defined in the remote / local state
	phaseByKey         map[string]*model.Phase // KEY is ID for remote load and PATH for local load
	phaseDependsOnKeys map[string][]string     // phase KEY -> depends on KEYs
}

type PhasesCyclesError struct {
	cycles []string
}

func (v PhasesCyclesError) Error() string {
	return `found cycles in phases "dependsOn"`
}

// WriteError prints found cycles to the output.
func (v PhasesCyclesError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
	w.WritePrefix(level, v.Error(), trace)
	w.WriteNewLine()

	last := len(v.cycles) - 1
	for i, cycle := range v.cycles {
		w.WriteBullet(level + 1)
		w.Write(cycle)
		if i != last {
			w.WriteNewLine()
		}
	}
}

func newPhasesSorter() *phasesSorter {
	return &phasesSorter{
		phaseByKey:         make(map[string]*model.Phase),
		phaseDependsOnKeys: make(map[string][]string),
	}
}

func (s *phasesSorter) sortPhases() ([]*model.Phase, error) {
	errs := errors.NewMultiError()
	graph := &toposort.Sorter{}

	// Add dependencies to graph
	for _, key := range s.phasesKeys {
		graph.AddNode(key)
		for _, dependsOnKey := range s.phaseDependsOnKeys[key] {
			if s.phaseByKey[dependsOnKey] != nil {
				graph.AddEdge(key, dependsOnKey)
			}
		}
	}

	// Topological sort by dependencies
	order, cycles := graph.Sort()
	if len(cycles) > 0 {
		var cyclesAsStr []string
		for _, cycle := range cycles {
			var items []string
			for _, item := range cycle {
				items = append([]string{item.(string)}, items...) // prepend
			}
			cyclesAsStr = append(cyclesAsStr, strings.Join(items, ` -> `))
		}
		errs.Append(&PhasesCyclesError{cycles: cyclesAsStr})
	}

	// Generate slice
	phases := make([]*model.Phase, 0, len(order))
	for phaseIndex, keyRaw := range order {
		key := keyRaw.(string)
		phase := s.phaseByKey[key]
		phase.Index = phaseIndex
		for taskIndex, task := range phase.Tasks {
			task.TaskKey = model.TaskKey{
				PhaseKey: phase.PhaseKey,
				Index:    taskIndex,
			}
			phase.Tasks[taskIndex] = task
		}
		phases = append(phases, phase)
	}

	// Fill in "dependsOn"
	for _, keyRaw := range order {
		var dependsOn []*model.Phase
		key := keyRaw.(string)
		phase := s.phaseByKey[key]
		for _, dependsOnKey := range s.phaseDependsOnKeys[key] {
			dependsOnPhase, found := s.phaseByKey[dependsOnKey]
			if !found {
				errs.Append(errors.Errorf(`missing phase "%s", referenced from "%s"`, dependsOnKey, key))
				continue
			}
			dependsOn = append(dependsOn, dependsOnPhase)
		}

		// Sort dependsOn phases
		sort.SliceStable(dependsOn, func(i, j int) bool {
			return dependsOn[i].Index < dependsOn[j].Index
		})

		// Convert ID -> PhaseKey (index)
		phase.DependsOn = make([]model.PhaseKey, 0)
		for _, depPhase := range dependsOn {
			phase.DependsOn = append(phase.DependsOn, depPhase.PhaseKey)
		}
	}

	return phases, errs.ErrorOrNil()
}
