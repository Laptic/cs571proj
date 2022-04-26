package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/goombaio/dag"
)

func main() {

	start()

	//count := lineCounter("./amazonData/train.csv")
	//fmt.Println(count)
	//expFiles("pgfrank.txt", 3)
	//expFiles("sample.txt", 3)
	//expFiles("./amazonData/train.csv", 3)
	// commands, _ := readFile("./myinput/dag_input4.txt")
	// //fmt.Println(commands)
	// //data := PerformCommmand_experiment(commands[0], "pg-frankenstein.txt")
	// //writeToFile(data, "./filestore/exper1.txt")
	// data := PerformCommmand_experiment(commands[0], "pgfrank.txt")
	// writeToFile(data, "./filestore/exper1.txt")

	// cmd := exec.Command("awk", "/mili/ {print}", "pg-frankenstein.txt")
	// data, _ := cmd.Output()
	// writeToFile(data, "./filestore/something.txt")

}

func split_file_experiment(filename string, numChunks int) {

	file, err := os.Open(filename)

	stats, err := file.Stat()

	fileSize := stats.Size()

	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	numlines := lineCounter(filename)
	sizeofChunks := int64(numlines) / int64(numChunks)

	fmt.Println("sizeofFile")
	fmt.Println(fileSize)
	fmt.Println("Size of chunks")
	fmt.Println(sizeofChunks)
	fmt.Println("-----")

	currentFile := 0
	numLines := 0
	listOfText := []string{}
	totalChunks := 0

	for scanner.Scan() {
		linetext := scanner.Text()
		numLines += 1
		listOfText = append(listOfText, linetext)

		if numLines >= int(sizeofChunks) && totalChunks != (int(sizeofChunks)-1) {
			fileName := "./filestore/somebigfile_" + strconv.FormatInt(int64(currentFile), 10)
			filePtr, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			for _, text := range listOfText {
				_, err := filePtr.WriteString(text + "\n")
				if err != nil {
					log.Fatal(err)
				}
			}
			filePtr.Close()
			numLines = 0
			currentFile += 1
			listOfText = nil
			totalChunks += 1
		}
	}

	fileName := "./filestore/somebigfile_" + strconv.FormatInt(int64(currentFile), 10)
	filePtr, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	for _, text := range listOfText {
		_, err := filePtr.WriteString(text + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}
	filePtr.Close()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	file.Close()
}

func lineCounter(file string) int {
	fileptr, _ := os.Open(file)
	fileScanner := bufio.NewScanner(fileptr)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	return lineCount
}

func printall(args ...string) {

	for _, word := range args {
		fmt.Println(word)
	}
}

func start() {

	var inputText string = os.Args[1]

	var dagText string = os.Args[2]

	//commands, numberofCommands := readFile("./myinput/dag_input3.txt")
	commands, numberofCommands := readFile(inputText)
	dag1 := dag.NewDAG()

	matrix := createDag2(dag1, commands, numberofCommands)
	// fmt.Println(dag1)

	// fmt.Println(matrix)
	master := Master{make(chan string, 100)}
	//scheduler(matrix, dag1, master, "./amazonData/test.csv")
	scheduler(matrix, dag1, master, dagText)
	//duration := time.Since(start)
	//fmt.Println(duration)
	////////////////////
	// var files []string = []string{"./myinput/a.txt", "./myinput/b.txt", "./myinput/c.txt"}
	// mergeFiles(files, "grep")

	// //sourceverts := dag1.SourceVertices()
	// fmt.Println(dag1)
	//printDag(dag1, sourceverts)
	//RunCommmand("sed", "s/unix/linux/", "./myinput/input1.txt")
	//master.RunCommand("sed", "s/unix/linux/", "./myinput/input1.txt")
}
