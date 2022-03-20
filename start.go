package main

import "fmt"

func main() {

	//splitChunks("pg-frankenstein.txt", 10)
	wg.Add(1)
	master := Master{make(chan string, 10), make(chan string, 10)}

	master.addCommand("grep")
	//	master.addCommand("grep")
	master.addFile("pg-frankenstein.txt")

	master.schedule("no")

	fmt.Println(<-master.file)
	wg.Wait()
}
