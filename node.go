package shDB

import (
	"bytes"
	"encoding/binary"
)

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

// Insert a KKey functions--------------------------------------------------
// lookup for the the position
func nodeLookUpLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	//1st keey is a copy of parenty key which will be always equal or less than the parent key
	for i := uint16(1); i < nkeys; i++ {
		compare := bytes.Compare(node.getKey(i), key)
		if compare <= 0 {
			found = 1
		}
		if compare >= 0 {
			break
		}
	}
	return found
}

// adding a new KV to the leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, value []byte) {
	new.setHeader(BNode_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, value)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)

}

// copy KVs into the position (till idx then and after idx )
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	// Later todo - 2 assert statements
	if n == 0 {
		return
	}
	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// offsets
	dstBegin := new.getOffSet(dstNew)
	srcBegin := old.getOffSet(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffSet(srcOld+i) - srcBegin
		new.setOffSet(dstNew+i, offset)
	}
	// KVs
	begin := old.KvPos(srcOld)
	end := old.KvPos(srcOld + n)
	copy(new.Data[new.KvPos(dstNew):], old.Data[begin:end])
}

// copy new KV
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.KvPos(idx)
	binary.LittleEndian.PutUint16(new.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.Data[pos+2:], uint16(len(val)))
	copy(new.Data[pos+4:], key)
	copy(new.Data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffSet(idx+1, new.getOffSet(idx)+4+uint16((len(key)+len(val))))
}
