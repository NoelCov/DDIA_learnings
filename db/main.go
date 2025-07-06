package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type LogbasedDB struct {
	segmentFileName  string
	segmentsFileName string
	indexTree        *rbt.Tree
}

func main() {
	db := LogbasedDB{"", "segments.manifest", rbt.NewWithStringComparator()}
	db.init()

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

		segmentFileNames := db.getSegmentFileNames()
		// Latest file should always be checked first, then second latest, etc. This is because we want to find the latest key/value pair that exists in case it was updated.
		for i := len(segmentFileNames) - 1; i >= 0; i-- {
			segmentFile, err := os.Open("segments/" + segmentFileNames[i] + ".txt")
			if err != nil {
				fmt.Printf("Error opening segment file '%s': %s", segmentFileNames[i], err)
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
		}
		fmt.Println("\nKey doesn't exist in db, searched all segment files.")
	}
}

func (db *LogbasedDB) getStateOfTree() {
	fmt.Println(db.indexTree)
}

func (db *LogbasedDB) generateSegmentFileFromTree() {
	dst, err := os.OpenFile("segments/"+db.segmentFileName+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error opening segment file to write to: ", err)
	}
	defer dst.Close()

	iterator := db.indexTree.Iterator()
	for iterator.Next() {
		dst.WriteString(fmt.Sprintf("%s=%s\n", strings.TrimSpace(iterator.Key().(string)), strings.TrimSpace(iterator.Value().(string))))
	}
	db.indexTree = rbt.NewWithStringComparator()
	parts := strings.Split(db.segmentFileName, "-")
	sequenceNum, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatal("Could not parse integer from sequence number of segment file.")
	}
	// Update the values for the next segment file to be written.
	sequenceNum += 1
	db.segmentFileName = "segment-" + strconv.Itoa(sequenceNum)
	fmt.Println("Segment file was created.\n")
}

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

// TODO be aware of how many times I'm adding logic to read from segments/ check if there is a way to do this easier.
func (db *LogbasedDB) init() {
	// Get the name of the manifest file and create it if not exists
	segmentsFileName := db.segmentsFileName
	_, err := os.Stat(segmentsFileName)
	if os.IsNotExist(err) {
		_, err := os.Create(segmentsFileName)
		if err != nil {
			log.Fatal("Could not create segments file: ", err)
		}
	}

	segmentsFile, err := os.OpenFile(segmentsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error opening segments file: ", err)
	}

	// Scan segments file to see if there are any segments, if not create first one, otherwise read the name of the last one to use it
	scanner := bufio.NewScanner(segmentsFile)
	segments := make([]string, 0)
	for scanner.Scan() {
		segments = append(segments, scanner.Text())
	}

	segmentFileName := "segment-0001"
	if len(segments) == 0 {
		_, err := os.Create("segments/" + segmentFileName + ".txt")
		if err != nil {
			log.Fatal("Error creating segment file: ", err)
		}
		_, err = segmentsFile.WriteString(segmentFileName + "\n")
		if err != nil {
			log.Fatal("Could not write to segment file: ", err)
		}
		db.segmentFileName = segmentFileName
	} else {
		db.segmentFileName = segments[len(segments)-1]
	}

	fmt.Println("Database was initialized correctly.\n")
}

func (db *LogbasedDB) getSegmentFileNames() []string {
	segmentsFile, err := os.Open(db.segmentsFileName)
	if err != nil {
		log.Fatal("Error opening segments manifest file: ", err)
	}

	// Scan segments file to see if there are any segments, if not create first one, otherwise read the name of the last one to use it
	scanner := bufio.NewScanner(segmentsFile)
	segments := make([]string, 0)
	for scanner.Scan() {
		segments = append(segments, scanner.Text())
	}
	return segments
}
