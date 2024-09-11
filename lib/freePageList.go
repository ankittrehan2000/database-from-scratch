package lib

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
