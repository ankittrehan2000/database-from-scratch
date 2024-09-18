package lib

import (
	"bytes"
	"encoding/binary"
)

// Create a node class for the b-tree to store data
type Item struct {
	Key   []byte
	Value []byte
}

type Node struct {
	*Dal

	pageNum    pgnum
	Items      []*Item
	childNodes []pgnum
}

func newEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		Key:   key,
		Value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

// Each node in the b-tree gets some header information about itself (isLeaf, number of Items etc)
// Then for each item (key-value pair) store its offset within the node buffer and store the key value pair starting at that offset
func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	// Add page header information
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.Items)))
	leftPos += 2

	for i := 0; i < len(n.Items); i++ {
		item := n.Items[i]
		if !isLeaf {
			childNode := n.childNodes[i]
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.Key)
		vlen := len(item.Value)

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.Value)
		rightPos -= 1

		buf[rightPos] = byte(vlen)
		rightPos -= klen
		copy(buf[rightPos:], item.Key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	// Get header information
	leftPos := 0
	isLeaf := uint16(buf[0])
	ItemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))

	// Get key-value pairs
	for i := 0; i < ItemsCount; i++ {
		if isLeaf == 0 {
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])

		value := buf[offset : offset+vlen]
		offset += vlen
		n.Items = append(n.Items, newItem(key, value))
	}
}

func (n *Node) writeNode(node *Node) (*Node, error) {
	return n.Dal.writeNode(node)
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.Dal.GetNode(pageNum)
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.Items {
		res := bytes.Compare(existingItem.Key, key)
		if res == 0 {
			return true, i
		}

		// The key is bigger than the node so it might exist in child node
		if res == 1 {
			return false, i
		}
	}

	return false, len(n.Items)
}

func (n *Node) FindKey(key []byte) (int, *Node, error) {
	index, node, err := findKeyHelper(n, key)
	if err != nil {
		return -1, nil, err
	}
	return index, node, err
}

func findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	// search for key inside the node
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	// if leaf nodes are reached it means the key wasn't found and must not exist
	if node.isLeaf() {
		return -1, nil, nil
	}

	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key)
}
