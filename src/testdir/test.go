package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {

	fmt.Println("main start")

	x := 1
	var mu sync.Mutex

	mu.Lock()
	fmt.Println("main locked")
	defer mu.Unlock()

	fmt.Println("main start goroutine")
	go func() {

		fmt.Println("goroutine try lock")
		mu.Lock()
		fmt.Println("goroutine locked")
		defer mu.Unlock()

		x++
		fmt.Println("goroutine unlocked")

	}()

	fmt.Println("main wait")
	// 添加 sleep 给 goroutine 时间执行
	time.Sleep(5 * time.Second)
}
