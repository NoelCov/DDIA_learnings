package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

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

	for {
		selection := getMenuSelection()
		if selection == "1" {
			db.getKeyValue()
		} else if selection == "2" {
			db.writeKeyValue()
		} else if selection == "3" {
			db.getStateOfTree()
		} else {
			return
		}
	}
}

func (db *LogbasedDB) writeKeyValue() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nEnter your key: ")
	key, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Error reading key input.")
	}

	fmt.Print("Enter your value: ")
	value, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Error reading value input.")
	}

	indexTree := db.indexTree
	indexTree.Put(key, value)

	fmt.Println("Value was inserted in database.\n")

	if indexTree.Size() == 2 {
		db.generateSegmentFileFromTree()
		db.indexTree = rbt.NewWithStringComparator()
	}

}

func (db LogbasedDB) getKeyValue() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nEnter the key to search for: ")
	key, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Error reading key input.")
	}

	keyValue, found := db.indexTree.Get(key)
	if found {
		fmt.Println(keyValue.(string))
	} else {
		fmt.Println("The key wasn't found in the tree. Looking into segment files")

		segmentFile, err := os.Open("segment.txt")
		if err != nil {
			log.Fatal("Error opening segment file.")
		}
		defer segmentFile.Close()

		lines := make([]string, 0)
		scanner := bufio.NewScanner(segmentFile)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}

		left, right := 0, len(lines)-1
		fmt.Println(lines)
		key = strings.TrimSpace(key)

		for left <= right {
			mid := (left-right)/2 + right
			fmt.Println(mid)

			parts := strings.Split(lines[mid], "=")
			currKey := parts[0]
			if key == currKey {
				fmt.Printf("\nValue for key '%s' is: %s\n\n", key, parts[1])
				return
			} else if key > currKey {
				left = mid + 1
			} else {
				right = mid - 1
			}
		}
		fmt.Println("\nKey doesn't exist.")
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
		dst.WriteString(fmt.Sprintf("%s=%s\n", strings.TrimSpace(iterator.Key().(string)), strings.TrimSpace(iterator.Value().(string))))
	}
	fmt.Println("Segment file was created.")
}

// TODO Make a menu struct for this
func getMenuSelection() string {
	fmt.Println("Select one of the options below:\n")

	fmt.Println("1 - Search for key in database.")
	fmt.Println("2 - Enter key/value pair in database.")
	fmt.Println("3 - Look at state of database.")
	fmt.Println("4 - Quit application.")

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nEnter your selection: ")
	selection, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("Couldn't get menu selection.")
	}
	return strings.TrimSpace(selection)
}
