# LSM-Tree attempt xD

The third chapter of DDIA was my favorite, I have bene using databases since the start of my career but I feel like I never even attempted to understand how databases store, retrieve, update the data we give it. This chapter showed me how amazing these systems are, and I wanted to attempt to make a LSM-Tree to make sure I learned some of the concepts this chapter introduced, such as:
- Log-based storage
- Usage of red-black trees to store key/value pairs in order to be able to do retrievals in O(log n)
- Segment files (generation, compacting of them)

## Running the app
You can download the code source, and with golang use ```go run main.go``` which will open up a menu to give you options to insert data into the db, view the state of the tree, search for a key in the db.

