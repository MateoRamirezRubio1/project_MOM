package main

import "time"

func main() {
	_ = buildServer() // goroutine already serving
	for {
		time.Sleep(time.Hour)
	} // block main
}
