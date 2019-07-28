package main

import (
	"redis_like_in_memory_db/internal/bucket"
)

const defaultNumberOfBuckets = 32

type hashFunc func(string) uint

type stringCache struct {
	hashFunc
	buckets []*bucket.Bucket
}


func main() {
}

// SDBM hash function implemented in golang
// http://www.partow.net/programming/hashfunctions/index.html#SDBMHashFunction
func bucketHashFunc(numBuckets uint) func(string) uint {
	return func(key string) uint {
		var hash uint64 = 0
		for _, char := range key {
			hash = uint64(char) + (hash << 6) + (hash << 16) - hash
		}
		return uint(hash % uint64(numBuckets))
	}
}

func newStringCache() *stringCache {
	cache := new(stringCache)
	cache.hashFunc = bucketHashFunc(defaultNumberOfBuckets)
	cache.buckets = make([]*bucket.Bucket, defaultNumberOfBuckets, defaultNumberOfBuckets)
	for i := 0; i < defaultNumberOfBuckets; i++ {
		cache.buckets[i] = bucket.NewBucket()
	}
	return cache
}

