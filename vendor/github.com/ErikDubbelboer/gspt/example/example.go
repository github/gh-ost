package main

import (
	"fmt"
	"strings"
	"time"

	"os"
	"os/exec"

	"github.com/erikdubbelboer/gspt"
)

func main() {
	fmt.Println("HaveSetProcTitle:", gspt.HaveSetProcTitle)
	fmt.Println("HaveSetProcTitleFast:", gspt.HaveSetProcTitleFast)

	gspt.SetProcTitle("some title")

	fmt.Println("The title has been set, 'ps a' now shows:")

	out, err := exec.Command("ps", "aux").Output()
	if err != nil {
		// Could not execute 'ps'.
		return
	}
	lines := strings.Split(string(out), "\n")
	fmt.Println(lines[0])
	for _, line := range lines {
		if strings.Contains(line, "some title") {
			fmt.Println(line)
		}
	}

	fmt.Println("----")
	fmt.Println("os.Environ() should still contain things like:")
	fmt.Println("PATH =", os.Getenv("PATH"))

	fmt.Println("----")
	fmt.Println("os.Args should still be correct:")

	for i, a := range os.Args {
		fmt.Println(i, ":", a)
	}

	fmt.Println("----")
	fmt.Println("You can now check the output of 'ps a' or 'top'.")
	fmt.Println("Press Ctrl+C to kill this program.")

	time.Sleep(time.Minute * 5)
}
