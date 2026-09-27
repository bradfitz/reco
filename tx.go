package reco

import "fmt"

// Tx is a serialized per-graph transaction.
type Tx struct {
	g      *Graph
	writes map[*nodeDef]any
}

// Set assigns a scalar data node within a transaction.
func Set[T any](tx *Tx, node Node[T], value T) {
	tx.mustData(node.def)
	tx.writes[node.def] = value
}

func (tx *Tx) mustData(def *nodeDef) {
	if def == nil {
		panic("reco: zero node handle")
	}
	if def.kind != nodeData {
		panic(fmt.Sprintf("reco: node %s is not a data node", def.className))
	}
}

func (tx *Tx) current(def *nodeDef) nodeValue {
	if v, ok := tx.writes[def]; ok {
		return nodeValue{value: v, valid: true}
	}
	return tx.g.nodes[def]
}
