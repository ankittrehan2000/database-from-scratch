package lib

import "encoding/binary"

// Create a node class for the b-tree to store data
type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*Dal

	pageNum    pgnum
	items      []*Item
	childNodes []pgnum
}

func newEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

// Each node in the b-tree gets some header information about itself (isLeaf, number of items etc)
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

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)
		rightPos -= 1

		buf[rightPos] = byte(vlen)
		rightPos -= klen
		copy(buf[rightPos:], item.key)

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
	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))

	// Get key-value pairs
	for i := 0; i < itemsCount; i++ {
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
		n.items = append(n.items, newItem(key, value))
	}
}

func (n *Node) writeNode(node *Node) (*Node, error) {
	return n.Dal.writeNode(node)
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.Dal.getNode(pageNum)
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}
