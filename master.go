package main

import (
	"math/rand"
	"sync"
	"time"
)

type Master struct {
	file    chan string
	command chan string
}

var wg sync.WaitGroup

func (mr *Master) addCommand(command string) {

	mr.command <- command
}

func (mr *Master) addFile(file string) {

	mr.file <- file
}

//wg will need to have had called the add() method before calling the run command
func (mr *Master) runCommand(command string, input string, fileIn string) {

	data := PerformCommmand(command, input, fileIn)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	filename := randomInputFileName(r1.Intn(1000000))

	writeToFile(data, filename)

	//The 2 value will need to be removed and replaced with the number of children a particular dag node has
	filenames := splitChunks(filename, 2)

	//add the names of the file chunks
	for _, s := range filenames {
		mr.addFile(s)
	}

	wg.Done()
}

//input will usually be grabbed from the command, command may be changed to a tuple
func (mr *Master) schedule(input string) {

	command := <-mr.command
	file := <-mr.file

	go mr.runCommand(command, input, file)

}
