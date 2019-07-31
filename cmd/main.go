package main

import (
	"flag"
	"redis_like_in_memory_db/internal/server"
	"runtime"
)

func main() {
	auth := flag.Bool("auth", false, "Wether to require password on session beginning")
	numBuckets := flag.Int("num_buckets", 32, "Number of buckets for each type of bucket")
	port := flag.String("port", ":8000", "Port number with suffix colon")
	enableLogging := flag.Bool("logging", true, "enable commands logging")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	server := server.NewServer(*port, *auth, *enableLogging, "password", *numBuckets)
	server.Run()
}
