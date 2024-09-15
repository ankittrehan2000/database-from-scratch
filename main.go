package main

import (
	"database-from-scratch/lib"
)

func main() {
	dal, _ := lib.NewDal("db.db")

	p := dal.AllocatePage()
	p.Num = dal.GetNextPage()
	copy(p.Data[:], "data")
	_ = dal.WritePage(p)

	_ = dal.WritePage(p)
	_, _ = dal.WriteFreeList()

	_ = dal.Close()

	// We expect the freelist state was saved, so we write to
	// page number 3 and not overwrite the one at number 2
	dal, _ = lib.NewDal("db.db")
	p = dal.AllocatePage()
	p.Num = dal.GetNextPage()
	copy(p.Data[:], "data2")
	_ = dal.WritePage(p)

	// Create a page and free it so the released pages will be updated
	pageNum := dal.GetNextPage()
	dal.ReleasePage(pageNum)

	// commit it
	_, _ = dal.WriteFreeList()
}
