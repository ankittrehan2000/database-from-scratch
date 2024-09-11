package lib

import (
	"fmt"
	"os"
)

type pgnum uint64

type page struct {
	Num  pgnum
	Data []byte
}

type Dal struct {
	file     *os.File
	pageSize int
	*freePageList
}

func NewDal(path string, pageSize int) (*Dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dal := &Dal{file, pageSize, newFreePageList()}
	return dal, nil
}

func (d *Dal) close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
	}
	return nil
}

func (d *Dal) AllocatePage() *page {
	return &page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *Dal) ReadPage(pageNum pgnum) (*page, error) {
	p := d.AllocatePage()

	offset := int(pageNum) * d.pageSize
	_, err := d.file.ReadAt(p.Data, int64(offset))
	if err != nil {
		return nil, err
	}
	return p, err
}

func (d *Dal) WritePage(p *page) error {
	offset := int64(p.Num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.Data, offset)
	return err
}
