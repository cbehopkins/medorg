package main

import "github.com/cbehopkins/medorg"

func main() {
	directory := "."

	tu := medorg.NewTreeUpdate(2, 2)

	tu.UpdateDirectory(directory)

}
