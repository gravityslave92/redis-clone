package main

import (
	"flag"
	server "redis_like_in_memory_db/internal/server"
)

func main()  {
	numGoRoutines := flag.Int("numGoRoutines", 64, "Number of  simultaneous request to the cache.")
	numBuckets := flag.Int("numBuckets", 64, "Number of buckets for every bucket type")
	flag.Parse()
	
	server := server.NewServer(":8888", false, "", *numBuckets)
}
