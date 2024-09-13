package lib

import "encoding/binary"

// Free page list keeps track of all the pages allocated for the database to avoid allocating unnecessary memory
type freePageList struct {
	maxPage       pgnum   // holds the last page number
	releasedPages []pgnum // holds the numbers of the pages that have been freed
}

func newFreePageList() *freePageList {
	return &freePageList{
		maxPage:       0,
		releasedPages: []pgnum{},
	}
}

func (f *freePageList) GetNextPage() pgnum {
	if len(f.releasedPages) != 0 {
		pageId := f.releasedPages[f.releasedPages[len(f.releasedPages)-1]]
		return pageId
	}
	f.maxPage += 1
	return f.maxPage
}

func (f *freePageList) ReleasePage(page pgnum) {
	f.releasedPages = append(f.releasedPages, page)
}

func (f *freePageList) serialize(buf []byte) []byte {
	pos := 0
	binary.LittleEndian.PutUint16(buf[pos:], uint16(f.maxPage))

	// released page count
	pos += 2
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(f.releasedPages)))

	pos += 2
	for _, page := range f.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf
}

func (f *freePageList) deserialize(buf []byte) {
	pos := 0
	f.maxPage = pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	releasedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCount; i++ {
		f.releasedPages = append(f.releasedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
