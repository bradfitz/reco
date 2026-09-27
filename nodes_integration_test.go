package reco_test

import (
	"testing"

	"github.com/bradfitz/reco"
	"github.com/bradfitz/reco/nodes"
)

var standardNodes = reco.OperatorTestFuncs{
	Union:  nodes.Union[int],
	MapSet: nodes.MapSet[int, int],
}

// Keep the white-box storage and compute-count checks in reco's test build,
// but supply the actual nodes from outside the package to avoid an import cycle.
func TestCollectionDeltaWork(t *testing.T) {
	reco.CheckCollectionDeltaWork(t, standardNodes)
}

func TestStructNodeDeltaWork(t *testing.T) {
	reco.CheckStructNodeWork(t, nodes.Struct[reco.StructWorkRecord])
}

func TestCollectionStructuralSharing(t *testing.T) {
	reco.CheckCollectionStructuralSharing(t, standardNodes)
}

func TestMapSetDirectDeltaAndNetZeroBatches(t *testing.T) {
	reco.CheckMapSetDirectDeltaAndNetZeroBatches(t, standardNodes)
}

func BenchmarkCollectionDelta(b *testing.B) {
	reco.BenchCollectionDelta(b, standardNodes)
}

var setAlgebraNodes = []struct {
	name  string
	build reco.SetOperationTestFunc
}{
	{"Union", nodes.Union[int]},
	{"Intersection", nodes.Intersection[int]},
	{"Xor", nodes.Xor[int]},
	{"Difference", func(className reco.NodeClassName, in ...reco.Node[reco.SetSnapshot[int]]) reco.Node[reco.SetSnapshot[int]] {
		return nodes.Difference(className, in[0], in[1:]...)
	}},
}

var mapProjectionNodes = []struct {
	name  string
	build reco.MapProjectionTestFunc
}{
	{"MapKeys", nodes.MapKeys[int, int]},
	{"MapValues", nodes.MapValues[int, int]},
}

func TestSetOperationDeltaWork(t *testing.T) {
	for _, op := range setAlgebraNodes {
		t.Run(op.name, func(t *testing.T) { reco.CheckSetOperationDeltaWork(t, op.build) })
	}
}

func TestMapProjectionDeltaWork(t *testing.T) {
	for _, op := range mapProjectionNodes {
		t.Run(op.name, func(t *testing.T) { reco.CheckMapProjectionDeltaWork(t, op.build, op.name == "MapKeys") })
	}
}

func BenchmarkSetOperationDelta(b *testing.B) {
	for _, op := range setAlgebraNodes {
		b.Run(op.name, func(b *testing.B) { reco.BenchSetOperationDelta(b, op.build, op.name == "Intersection") })
	}
}

func BenchmarkMapProjectionDelta(b *testing.B) {
	for _, op := range mapProjectionNodes {
		b.Run(op.name, func(b *testing.B) { reco.BenchMapProjectionDelta(b, op.build) })
	}
}
