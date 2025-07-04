package main

import (
	"fmt"
	"log"
	"os"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type LogbasedDB struct {
	dataFileName string
	indexTree    *rbt.Tree
}

func main() {
	_, err := os.Create("data.txt")
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}
	db := LogbasedDB{"data.txt", rbt.NewWithStringComparator()}

	db.writeKeyValue("aaa", "value1")
	db.getKeyValue("aaa")

	db.writeKeyValue("ccc", "value2")

	db.writeKeyValue("calisthenicsKing", "owo")
	db.getStateOfTree()
}

func (db *LogbasedDB) writeKeyValue(key, value string) {
	indexTree := db.indexTree
	indexTree.Put(key, value)

	if indexTree.Size() == 2 {
		db.generateSegmentFileFromTree()
		db.indexTree = rbt.NewWithStringComparator()
	}
}

// TODO Add logic here to look into segment files if key is not found in memo.S
func (db LogbasedDB) getKeyValue(key string) {
	fmt.Println("Key to search for: ", key)
	keyValue, found := db.indexTree.Get(key)
	if found {
		fmt.Println(keyValue)
	} else {
		fmt.Println("The key wasn't found in the tree.")
	}

}

func (db *LogbasedDB) getStateOfTree() {
	fmt.Println(db.indexTree)
}

func (db *LogbasedDB) generateSegmentFileFromTree() {
	dst, err := os.Create("segment.txt")
	if err != nil {
		log.Fatal("Error opening segment file to write to.")
	}
	defer dst.Close()

	iterator := db.indexTree.Iterator()
	for iterator.Next() {
		dst.WriteString(fmt.Sprintf("%s=%s\n", iterator.Key(), iterator.Value()))
	}
	fmt.Println("Segment file was created.")
}
