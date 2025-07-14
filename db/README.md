# LSM-Tree

The third chapter of DDIA was my favorite, I have been using databases since the start of my career but I feel like I never even attempted to understand how databases store, retrieve, update the data we give it. This chapter showed me how amazing these systems are, and I wanted to attempt to make a LSM-Tree to make sure I learned some of the concepts this chapter introduced, such as:
- Log-based storage databases
- Usage of red-black trees to store key/value pairs in order to be able to do retrievals in O(log n)
- Segment files (generation, compaction of them)

## Running the app
You can download the source code, and with golang use ```go run main.go``` which will open up a menu to give you options to insert data into the db, view the state of the tree, search for a key in the db, delete a key, and compact files.

## Logic
### Logic behind segments & compaction
The logic here is fairly simple, and I made it simple to be able to easily test it. The application will make a segment file and once the file has two values it will start a new one, every 2 segment files it will merge them into one. 

### Logic behind insertion
The logic behind insertion is as with any LSM-Tree, keys go into a red-black tree or similar, a data structure that keep keys in order to be able to retrieve in O(log n), when it gets to a certain treshold (2 values), it sends the keys into a segment file and restarts the red-black tree.

### Logic behind deletion
For deletions I learned that the way log-based databases and other systems do is by adding a deletion marker and then doing the deletion during compaction, which is what I decided to do, I'm adding a deletion marker (__TOMBSTONE__) to keys that will be deleted during compaction.

### Logic behind searching
For searching is also fairly simple, look at the LSM-Tree first, if the key isn't there, start searching the segments, starting from the most recent one to the oldest one. Log whether the key was found or not. This is done in O(n log n) worst case, if we have to look at every single segment file, best case is O(log n).
