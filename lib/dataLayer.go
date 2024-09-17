package lib

import (
	"errors"
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

func NewDal(path string) (*Dal, error) {
	dal := &Dal{
		meta:     newEmptyMeta(),
		pageSize: os.Getpagesize(),
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freePageList, err := dal.readFreeList()
		if err != nil {
			return nil, err
		}
		dal.freePageList = freePageList
	} else if errors.Is(err, os.ErrNotExist) {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.freePageList = newFreePageList()
		dal.freePageListNum = dal.GetNextPage()
		_, err := dal.WriteFreeList()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMeta(dal.meta)
	} else {
		return nil, err
	}

	return dal, nil
}

func (d *Dal) Close() error {
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

func (d *Dal) readFreeList() (*freePageList, error) {
	p, err := d.ReadPage(d.freePageListNum)
	if err != nil {
		return nil, err
	}

	freeList := newFreePageList()
	freeList.deserialize(p.Data)
	return freeList, nil
}

func (d *Dal) WriteFreeList() (*page, error) {
	p := d.AllocatePage()
	p.Num = d.freePageListNum
	d.freePageList.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}

	d.freePageListNum = p.Num
	return p, nil
}

func (d *Dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := newEmptyNode()
	node.deserialize(p.Data)
	node.pageNum = pageNum
	return node, nil
}

func (d *Dal) writeNode(node *Node) (*Node, error) {
	p := d.AllocatePage()
	if node.pageNum == 0 {
		p.Num = d.GetNextPage()
		node.pageNum = p.Num
	} else {
		p.Num = node.pageNum
	}

	p.Data = node.serialize(p.Data)
	err := d.WritePage(p)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *Dal) deleteNode(pageNum pgnum) {
	d.ReleasePage(pageNum)
}
