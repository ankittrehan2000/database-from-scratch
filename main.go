package main

import (
	"database-from-scratch/lib"
	"fmt"
)

func main() {
	dal, err := lib.NewDal("mainTest")

	if err != nil {
		fmt.Println(err)
	}
	node, _ := dal.GetNode(dal.Root)

	node.Dal = dal
	index, containingNode, err := node.FindKey([]byte("Key1"))

	if err != nil {
		fmt.Println(err)
	}
	res := containingNode.Items[index]
	fmt.Printf("key is %s, value is %s", res.Key, res.Value)
	_ = dal.Close()
}
