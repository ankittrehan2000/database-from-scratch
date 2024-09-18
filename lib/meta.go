package lib

import "encoding/binary"

const (
	metaPageNum = 0
)

// go doesn't have great support for converting structs to binary
// hence we need serialize and deserialize functions
type meta struct {
	Root            pgnum
	freePageListNum pgnum
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.Root))
	pos += pageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freePageListNum))
	pos += pageNumSize
}

func (m *meta) deserialize(buf []byte) {
	pos := 0

	m.Root = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.freePageListNum = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
