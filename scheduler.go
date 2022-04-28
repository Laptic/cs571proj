package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/goombaio/dag"
)

var wgOne sync.WaitGroup

func scheduler(dagMatrix [][]*dag.Vertex, dag *dag.DAG, ms Master, fileInput string) {

	numSourceNodes := len(dag.SourceVertices())

	fmt.Println("Starting splitting")

	if numSourceNodes > 1 {
		listOfSourceFiles := splitChunks_experiment(fileInput, "schedulerInput", "00", numSourceNodes)

		for _, file := range listOfSourceFiles {
			ms.files <- file
		}
	} else {
		ms.files <- fileInput
	}

	fmt.Println("Ending splitting")
	//////////////////////////////

	////////////////////////////
	for i := 0; i < len(dagMatrix); i++ {
		count := 0
		for j := 0; j < len(dagMatrix[i]); j++ {

			//fmt.Println(dagCommand[i][j])

			if dagMatrix[i][j] == nil {
				continue
			}

			wgOne.Add(1)
			// fmt.Println(dagMatrix[i][j].OutDegree())
			// fmt.Println(dagMatrix[i][j].InDegree())

			//go performAction(dagMatrix[i][j], ms, dagMatrix[i][j].InDegree(), dagMatrix[i][j].OutDegree())
			go performAction_experiment(dagMatrix[i][j], ms, dagMatrix[i][j].InDegree(), dagMatrix[i][j].OutDegree())
			count += 1
		}

		//waitOne would hold the number of elements in a row
		//that way we wait until all those elements in a row are done
		//fmt.Println("Done waiting in this section")
		wgOne.Wait()

	}

	//fmt.Println(len(ms.files))
	close(ms.files)
	ms.merge()
	//////////////////////////////

	// performAction(dagMatrix[0][0], ms, 1)
	// wgOne.Add(1)
	// go performAction(nil, ms, 2)

	//wgOne.Wait()
	// fmt.Println("WE ARE DONE")
	//waitTwo where the value of wait is the total number of of commands

}

func performAction(vertexCommand *dag.Vertex, ms Master, numParent int, numChild int) {
	//fmt.Println(vertexCommand)

	fmt.Println(vertexCommand.Value)
	commandList := strings.Split(vertexCommand.Value.(string), " ")
	fmt.Println(commandList)
	value := commandList[1]
	if commandList[0] == "awk" {
		value = commandList[1][1:] + " " + commandList[2][:len(commandList[2])-1]
	}

	//assume numInput can be greater than 1
	//so we will need to keep that in mind

	var filename string
	var listofFilesToMerge []string
	if numParent > 1 {

		for i := 0; i < numParent; i++ {
			file := <-ms.files
			listofFilesToMerge = append(listofFilesToMerge, file)
		}

		//here we merge the files and place it in filename
		//we do NOT want to place this file in the channel, it is to be used to merge
		//different files (look at the lines after the else statement) and run a different command only
		fileMerge := mergeFiles(listofFilesToMerge, commandList[0])
		filename = ms.RunCommand(commandList[0], value, fileMerge)
	} else {
		file := <-ms.files
		//we do NOT want to place this file in the channel, it is to be used to merge
		//different files (look at the lines after the else statement) and run a different command only
		filename = ms.RunCommand(commandList[0], value, file)
	}

	// the list of files grabbed from if numParent > 1 should then be merged

	// ms.files <- filename
	var listofFiles []string
	if !(numChild == 0 || numChild == 1) {

		listofFiles = splitChunks(filename, commandList[0], vertexCommand.ID, numChild)

		for i := 0; i < len(listofFiles); i++ {
			ms.files <- listofFiles[i]
		}
	} else {
		ms.files <- filename
	}

	//fmt.Println(commandList)
	//ms.RunCommand()

	wgOne.Done()
}

func performAction_experiment(vertexCommand *dag.Vertex, ms Master, numParent int, numChild int) {
	//fmt.Println(vertexCommand)

	fmt.Println(vertexCommand.Value.(string))
	commandList := strings.Split(vertexCommand.Value.(string), " ")

	//assume numInput can be greater than 1
	//so we will need to keep that in mind

	var filename string
	var listofFilesToMerge []string
	if numParent > 1 {

		for i := 0; i < numParent; i++ {
			file := <-ms.files
			listofFilesToMerge = append(listofFilesToMerge, file)
		}

		//here we merge the files and place it in filename
		//we do NOT want to place this file in the channel, it is to be used to merge
		//different files (look at the lines after the else statement) and run a different command only
		fileMerge := mergeFiles(listofFilesToMerge, commandList[0])
		filename = ms.RunCommand_experiment(strings.Trim(vertexCommand.Value.(string), " "), fileMerge)
		fmt.Println(filename)
	} else {
		file := <-ms.files
		//we do NOT want to place this file in the channel, it is to be used to merge
		//different files (look at the lines after the else statement) and run a different command only
		filename = ms.RunCommand_experiment(strings.Trim(vertexCommand.Value.(string), " "), file)
		fmt.Println(filename)
	}

	// the list of files grabbed from if numParent > 1 should then be merged

	// ms.files <- filename
	var listofFiles []string
	if !(numChild == 0 || numChild == 1) {

		//listofFiles = splitChunks(filename, commandList[0], vertexCommand.ID, numChild)
		listofFiles = splitChunks_experiment(filename, commandList[0], vertexCommand.ID, numChild)
		for i := 0; i < len(listofFiles); i++ {
			ms.files <- listofFiles[i]
		}
	} else {
		ms.files <- filename
	}

	//fmt.Println(commandList)
	//ms.RunCommand()

	wgOne.Done()
}
