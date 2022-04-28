package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func PerformCommmand_experiment(commandString string, file string) []byte {

	fmt.Println(commandString)
	cmdStr := fmt.Sprintf(commandString, file)
	fmt.Println(cmdStr)
	//cmd := exec.Command(myCommandInputsarray[0], myCommandInputsarray[1:]...)
	////////////
	cmd := exec.Command("bash", "-c", cmdStr)
	fmt.Println("MY COMMAND IS:")
	fmt.Println(cmd)
	////////////
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + "::: " + stderr.String())
		panic(err)
	}

	return out.Bytes()
}

func PerformCommmand(command string, input string, fileIn string) []byte {

	cmd := exec.Command(command, input, fileIn)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
	}

	return out.Bytes()
}

func writeToFile(data []byte, filename string) {

	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	f.Write(data)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func splitChunks(filename string, commandString string, idPos string, chunks int) []string {

	split := chunks

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	texts := make([]string, 0)
	for scanner.Scan() {
		text := scanner.Text()
		texts = append(texts, text)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	var listofFileMade []string

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	lengthPerSplit := len(texts) / split
	for i := 0; i < split; i++ {
		if i+1 == split {
			chunkTexts := texts[i*lengthPerSplit:]
			filename := writefile(strings.Join(chunkTexts, "\n"), commandString, idPos, r1.Intn(1000000))
			listofFileMade = append(listofFileMade, filename)
		} else {
			chunkTexts := texts[i*lengthPerSplit : (i+1)*lengthPerSplit]
			filename := writefile(strings.Join(chunkTexts, "\n"), commandString, idPos, r1.Intn(1000000))
			listofFileMade = append(listofFileMade, filename)
		}
	}

	return listofFileMade
}

func splitChunks_experiment(filename string, commandString string, idPos string, numChunks int) []string {

	file, err := os.Open(filename)

	stats, err := file.Stat()

	fileSize := stats.Size()

	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	fmt.Println("Counting lines now")
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
	listofFiles := []string{}
	oldFilename := ""
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	for scanner.Scan() {
		linetext := scanner.Text()
		numLines += 1
		//fmt.Println(numLines)
		listOfText = append(listOfText, linetext)

		if numLines >= int(sizeofChunks) && totalChunks != (int(sizeofChunks)-1) {

			//fileName := "./filestore/somebigfile_" + strconv.FormatInt(int64(currentFile), 10)
			fileName := "./filestore/chunks-" + commandString + "_" + idPos + "_" + strconv.Itoa(r1.Intn(1000000)) + ".txt"
			oldFilename = fileName
			listofFiles = append(listofFiles, fileName)
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
	fmt.Println("Finished splitting part 1")

	fileName := oldFilename
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
	fmt.Println("Finished splitting part 2")
	file.Close()

	return listofFiles
}

func writefile(data string, commandString string, idPos string, uniqueid int) string {

	file, err := os.Create("./filestore/chunks-" + commandString + "_" + idPos + "_" + strconv.Itoa(uniqueid) + ".txt")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	file.WriteString(data)

	return file.Name()
}

func randomInputFileName(uniqueid int, command string) string {

	nameofFile := "input_" + command + "_" + strconv.Itoa(uniqueid) + ".txt"

	return nameofFile
}

func mergeFiles(files []string, commandString string) string {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	uniqueid := r1.Intn(1000000)

	mergedFileName := "./filestore/" + "mergeFile" + "_for_" + commandString + "_" + strconv.Itoa(uniqueid) + ".txt"

	f, err := os.OpenFile(mergedFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer

	for _, file := range files {
		b, err := ioutil.ReadFile(file)
		if err != nil {
			panic(err)
		}

		buf.Write(b)
		f.Write(buf.Bytes())
		f.Write([]byte("\n"))
		buf.Reset()
	}
	return mergedFileName
}
