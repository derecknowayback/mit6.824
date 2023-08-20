package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func main() {
	for i := 0; i < 2000; i++ {
		fmt.Printf("\nTIMES: %d\n", i)
		fmt.Printf("NOW: %v\n", time.Now())
		i++
		command := exec.Command("bash", "/Users/chenmengshu/Code/6.5840/src/main/test-mr.sh")
		command.Dir = "/Users/chenmengshu/Code/6.5840/src/main"
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		err := command.Run()
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}
