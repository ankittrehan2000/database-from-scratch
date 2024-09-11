package main

import (
	"database-from-scratch/lib"
	"os"
)

func main() {
	dal, _ := lib.NewDal("db.db", os.Getpagesize())

	p := dal.AllocatePage()
	p.Num = dal.GetNextPage()
	copy(p.Data[:], "data")
	_ = dal.WritePage(p)
}
