package medorg

// ModifyFunc is what is called duriong the walk to allow modification of the fs
type ModifyFunc func(dir, fn string, fs FileStruct) (FileStruct, bool)

// WalkingFunc A walking funciton is one that walks the tree - it will probably recurse
type WalkingFunc func(dir string, wkf WalkingFunc)
