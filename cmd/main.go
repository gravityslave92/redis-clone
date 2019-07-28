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
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	server := server.NewServer(*port, *auth, "password", *numBuckets)
	server.Run()
}
