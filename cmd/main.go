package main

import (
	"redis_like_in_memory_db/internal/server"
	"runtime"
)

func main()  {
	runtime.GOMAXPROCS(runtime.NumCPU())
	
	server := server.NewServer(":8000", true, "password")
	server.Run()
}
