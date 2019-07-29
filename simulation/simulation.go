package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"redis_like_in_memory_db/internal/global_cache"
	"runtime"
	"syscall"
	"time"
)

func main() {
	numGoRoutines := flag.Int("numGoRoutines", 64, "Number of  simultaneous request to the cache.")
	numBuckets := flag.Int("numBuckets", 64, "Number of buckets for every bucket type")
	flag.Parse()

	printConfig(*numBuckets, *numGoRoutines)

	cache := global_cache.NewCache(*numBuckets)
	payload := generateData()

	for i := 0; i < *numGoRoutines; i++ {
		go runTask(cache, &payload)
	}

	var i int
	var memStats runtime.MemStats

	// Register Signal for exiting program
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		printExit(i, &memStats)
		os.Exit(1)
	}()

	// Main program print stats every second
	for i = 0; i < 100000; i++ {
		printStats(&memStats)
		time.Sleep(time.Second * 1)
	}

}

func printConfig(numBuckets, numvRoutins int) {
	fmt.Println("--------------------------------------------")
	fmt.Println("Number of buckets ", numBuckets)
	fmt.Println("Number of goroutines ", numvRoutins)
	fmt.Println("--------------------------------------------")
}

func generateData() [9999999]string {
	var letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var resultArr [9999999]string
	count := 0
	for i := 0; i < len(letters); i++ {
		for j := 0; j < len(letters); j++ {
			for k := 0; k < len(letters); k++ {
				for l := 0; l < len(letters); l++ {
					var str = letters[i:i+1] + letters[j:j+1] + letters[k:k+1] + letters[l:l+1]
					resultArr[count] = str
					count++
				}
			}
		}
	}
	return resultArr
}

func runTask(cache *global_cache.GlobalCache, payload *[9999999]string) {
	key := payload[rand.Intn(1000000)]
	performSet(cache, key)

}

func performSet(cache *global_cache.GlobalCache, key string) {
	cache.ProcessCommand([]string{"SET", key, "100ms"})
}

//print message on program exit
func printExit(i int, memStats *runtime.MemStats) {
	fmt.Println()
	printStats(memStats)
	fmt.Println("--------------------------------------------")
	fmt.Println()
	fmt.Println("Exiting.")
	fmt.Println("Ran ", i, " times.")
	fmt.Println()

}

func printStats(memStats *runtime.MemStats) {
	runtime.ReadMemStats(memStats)
	fmt.Println("--------------------------------------------")
	fmt.Println("Alloc:\t\t\t", memStats.Alloc)
	fmt.Println("Sys:\t\t\t", memStats.Sys)
	fmt.Println("-----")
	fmt.Println("TotalAlloc:\t\t", memStats.TotalAlloc)
	fmt.Println("-----")
	fmt.Println("HeapAlloc:\t\t", memStats.HeapAlloc)
	fmt.Println("HeapSys:\t\t", memStats.HeapSys)
	fmt.Println("HeapIdle:\t\t", memStats.HeapIdle)
	fmt.Println("HeapInuse:\t\t", memStats.HeapInuse)
	fmt.Println("HeapReleased:\t\t", memStats.HeapReleased)
	fmt.Println("HeapObjects:\t\t", memStats.HeapObjects)
	fmt.Println("-----")
	fmt.Println("NextGC:\t\t", memStats.NextGC)
	fmt.Println("LastGC:\t\t", memStats.LastGC)
	fmt.Println("NumGC:\t\t", memStats.NumGC)

}
