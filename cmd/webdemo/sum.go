package main

import "github.com/bradfitz/reco"

// sumMap is a demo-local incremental reduction. project must be pure. State
// belongs to each graph; only changed entries contribute to an update's work.
func sumMap[K comparable, V any](className reco.NodeClassName, input reco.Node[reco.MapSnapshot[K, V]], project func(V) int) reco.Node[int] {
	return reco.Operator(className, []reco.Dependency{input}, func() reco.Compute[int] {
		var previous reco.MapSnapshot[K, V]
		total := 0
		return func(eval reco.Eval) reco.Result[int] {
			current := reco.Input(eval, input).Value()
			for change := range current.ChangesSince(previous) {
				if change.BeforeValid {
					total -= project(change.Before)
				}
				if change.AfterValid {
					total += project(change.After)
				}
			}
			previous = current
			return reco.OK(total)
		}
	})
}
