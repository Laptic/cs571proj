package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func PerformCommmand(command string, input string, fileIn string) (out []byte) {

	out, err := exec.Command(command, input, fileIn).Output()

	if err != nil {
		fmt.Printf("%s", err)
	}

	return out
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

func splitChunks(filename string, chunks int) []string {

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
			filename := writefile(strings.Join(chunkTexts, "\n"), r1.Intn(1000000))
			listofFileMade = append(listofFileMade, filename)
		} else {
			chunkTexts := texts[i*lengthPerSplit : (i+1)*lengthPerSplit]
			filename := writefile(strings.Join(chunkTexts, "\n"), r1.Intn(1000000))
			listofFileMade = append(listofFileMade, filename)
		}
	}

	return listofFileMade
}

func writefile(data string, uniqueid int) string {

	file, err := os.Create("chunks-" + strconv.Itoa(uniqueid) + ".txt")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	file.WriteString(data)

	return file.Name()
}

func randomInputFileName(uniqueid int) string {

	nameofFile := "input-" + strconv.Itoa(uniqueid) + ".txt"

	return nameofFile
}
