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
	*meta
}

func NewDal(path string, pageSize int) (*Dal, error) {
	// file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	// if err != nil {
	// 	return nil, err
	// }

	// dal := &Dal{file, pageSize, newFreePageList(), newEmptyMeta()}
	// return dal, nil

	dal := &Dal{
		meta: newEmptyMeta(),
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freeList, err := dal.readFreeList()
		if err != nil {
			return nil, err
		}
		dal.freeList = freeList
	}
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

func (d *Dal) writeMeta(meta *meta) (*page, error) {
	p := d.AllocatePage()
	p.Num = metaPageNum
	meta.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (d *Dal) readMeta() (*meta, error) {
	p, err := d.ReadPage(metaPageNum)
	if err != nil {
		return nil, err
	}
	meta := newEmptyMeta()
	meta.deserialize(p.Data)
	return meta, nil
}

func (d *Dal) writeFreeList() (*page, error) {
	p := d.AllocatePage()
	p.num := d.freeListPage
	d.freeList.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}

	d.freeListPage := p.num
	return p, nil
}
