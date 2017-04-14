package node

import "fmt"

func Run(id int,quit_ch chan int) {
	fmt.Println("Hello from node:",id)
	quit_ch <- 1
}
