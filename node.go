package shDB

import "encoding/binary"

type BNode struct {
	Data []byte
}

// Node Structure
// type | nKeys | pointers | offsets | Klen | Vlen | key | Value
//  2B  |   2B  | 8B*nKeys | 2B*nKeys|  2B  | 2B   | --- | -----

const (
	BNode_INT  = 1
	BNode_LEAF = 2
)

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max >= BTREE_MAX_KEY_SIZE {
		panic("node1max in too high")
	}
}

// Node Functions------------------------------------------------
// Header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.Data[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.Data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.Data[0:2], btype)
	binary.LittleEndian.PutUint16(node.Data[2:4], nkeys)
}

// Pointers
func (node BNode) getPtr(idx uint16) uint64 {
	if idx > node.nkeys() {
		panic("Index out of range")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.Data[pos : pos+8])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	if idx > node.nkeys() {
		panic("Index out of range")
	}
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.Data[pos:pos+8], val)
}

// Offsets
// (offsets are relative & offset of q=1st idx i.e. (idx 0) is noty storing as it will return always zero)
func offsetPos(node BNode, idx uint16) uint16 {
	if 1 >= idx && idx > node.nkeys() {
		panic("Index out of range")
	}
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffSet(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.Data[offsetPos(node, idx):])
}

func (node BNode) setOffSet(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.Data[offsetPos(node, idx):], offset)
}

// key - values
func (node BNode) KvPos(idx uint16) uint16 {
	if idx >= node.nkeys() {
		panic("Index out of range")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffSet(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := HEADER + 8*node.nkeys() + 2*node.nkeys() + node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos : pos+2])
	return node.Data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := HEADER + 8*node.nkeys() + 2*node.nkeys() + node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.Data[pos+2:])
	return node.Data[pos+4+klen:][:vlen]
}

// node size in Bytes
func (node BNode) nbytes() uint16 {
	return node.KvPos(node.nkeys())
}
