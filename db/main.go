package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type Database struct {
	// file     *os.File
	dataFile string
	index    map[string]int64
	tree     *rbt.Tree
}

func main() {
	memo := make(map[string]int64)
	_, err := os.Create("data.txt")
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}
	db := Database{"data.txt", memo, rbt.NewWithStringComparator()}

	db.writeKeyValueToTree("aaa", "value1")
	db.writeKeyValueToTree("ccc", "value2")
	db.writeKeyValueToTree("calisthenicsKing", "owo")

}

func (db *Database) writeKeyValue(key, value string) {
	file, err := os.OpenFile(db.dataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("There was an error opening the file: ", err)
	}

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatal("Error getting offset from file.")
	}

	str := fmt.Sprintf("%s=%s\n", key, value)
	_, err = file.WriteString(str)
	if err != nil {
		log.Fatal("Something went wrong while writing to file.", err)
	}

	db.index[key] = offset
}

func (db *Database) writeKeyValueToTree(key, value string) {
	tree := db.tree
	tree.Put(key, value)

	if tree.Size() == 2 {
		db.generateSegmentFileFromTree()
	}
}

func (db *Database) getStateOfTree() {
	fmt.Println(db.tree)
}

func (db *Database) getValue(key string) {
	offset, exists := db.index[key]
	if !exists {
		log.Fatal("Key does not exist in db.")
	}
	fmt.Println("Offset to read value from: ", offset)

	file, err := os.Open(db.dataFile)
	if err != nil {
		log.Fatal("There was an error opening the file: ", err)
	}
	defer file.Close()

	// Seek moves the pointer (cursor), offset is how many bytes to move, and whence is where to start.
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		log.Fatal("Error when seeking offset to read from.")
	}

	// Scanner is used to scan line by line, it starts the scanner at the cursor's location.
	scanner := bufio.NewScanner(file)

	scanner.Scan()

	line := scanner.Text()
	parts := strings.SplitN(line, "=", 2)

	if len(parts) == 2 {
		fmt.Println(parts[1])
	} else {
		log.Fatal("Parts weren't saved to database correctly.")
	}
}

// A segment is a batch.
// TODO Add logic to call this when current file gets to 5MB or something like that.
func (db *Database) generateSegmentFile() error {
	src, err := os.Open(db.dataFile)
	if err != nil {
		log.Fatal("Error opening source file.")
	}
	defer src.Close()

	dst, err := os.Create("segment.txt")
	if err != nil {
		log.Fatal("Error creating segment file.")
	}
	defer dst.Close()

	scanner := bufio.NewScanner(src)

	for scanner.Scan() {
		line := scanner.Text()
		_, err := dst.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	fmt.Println("Finished writing segment file.")
	return scanner.Err()
}

func (db *Database) generateSegmentFileFromTree() {
	dst, err := os.Create("segment.txt")
	if err != nil {
		log.Fatal("Error opening segment file to write to.")
	}
	defer dst.Close()

	iterator := db.tree.Iterator()
	for iterator.Next() {
		dst.WriteString(fmt.Sprintf("%s=%s\n", iterator.Key(), iterator.Value()))
	}
	fmt.Println("Segment file was created.")
}
