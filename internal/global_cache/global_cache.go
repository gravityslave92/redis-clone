package global_cache

import (
	"fmt"
	"redis_like_in_memory_db/internal/bucket"
	"redis_like_in_memory_db/internal/dict_bucket"
	"redis_like_in_memory_db/internal/list_bucket"
	"strings"
	"sync"
)

type hashFunc func(string) uint

type GlobalCache struct {
	hashFunc
	buckets     []*bucket.Bucket
	listBuckets []*list_bucket.ListBucket
	dictBuckets []*dict_bucket.DictBucket
}

type iBucket interface {
	Get(...string) (string, bool)
	Set(...string) error
	Keys(...string) string
	Len(...string) int
	Remove(...string) error
}

func NewCache(numBuckets int) *GlobalCache {
	cache := new(GlobalCache)
	cache.hashFunc = bucketHashFunc(numBuckets)
	cache.buckets = make([]*bucket.Bucket, numBuckets, numBuckets)
	cache.listBuckets = make([]*list_bucket.ListBucket, numBuckets, numBuckets)
	cache.dictBuckets = make([]*dict_bucket.DictBucket, numBuckets, numBuckets)

	for i := 0; i < numBuckets; i++ {
		cache.buckets[i] = bucket.NewBucket()
		cache.dictBuckets[i] = dict_bucket.NewBucket()
		cache.listBuckets[i] = list_bucket.NewBucket()
	}
	return cache
}

func (cache *GlobalCache) ProcessCommand(args []string) string {
	if len(args) < 1 {
		return "wrong arguments number\n"
	}

	firstArg := ""

	command := args[0]
	if len(args) > 1 {
		firstArg = args[1]
	}
	bucket := cache.pickBucket(command, firstArg)

	var reply string
	switch {
	case strings.HasSuffix(command, "GET"):
		if value, ok := bucket.Get(args[1:]...); ok {
			reply = value
		} else {
			reply = "value does not exist for given arguments"
		}
	case strings.HasSuffix(command, "SET"):
		if err := bucket.Set(args[1:]...); err != nil {
			reply = err.Error()
		} else {
			reply = "Success"
		}

	case strings.HasSuffix(command, "KEYS"):
		if firstArg == "" {
			reply = cache.totalBucketsKeys()
		} else {
			reply = bucket.Keys(args[1:]...)
		}

	case strings.HasSuffix(command, "LEN"):
		if firstArg == "" {
			reply = fmt.Sprintf("%d", cache.totalBucketsLen())
		} else {
			reply = fmt.Sprintf("%d", bucket.Len(args[1:]...))
		}

	case strings.HasSuffix(command, "REM"):
		if err := bucket.Remove(args[1:]...); err != nil {
			reply = err.Error()
		} else {
			reply = "Success"
		}

	default:
		reply = "Command not found"
	}

	return fmt.Sprintf("%s\n", reply)
}

func (cache *GlobalCache) pickBucket(command, key string) iBucket {
	bucketIndex := cache.hashFunc(key)
	switch {
	// ZGET ZSET ZLEN ZREM ZKEYS
	case strings.HasPrefix(command, "Z"):
		return cache.listBuckets[bucketIndex]
	// DGET DSET DLEN DREM DKEYS
	case strings.HasPrefix(command, "D"):
		return cache.dictBuckets[bucketIndex]
	// GET SET LEN REM KEYS
	default:
		return cache.buckets[bucketIndex]
	}
}

func (cache *GlobalCache) totalBucketsLen() int {
	wg := &sync.WaitGroup{}
	ch := make(chan int)

	go func() {
		for key := range cache.buckets {
			wg.Add(1)

			go func(buck *bucket.Bucket) {
				ch <- buck.Len()
				wg.Done()
			}(cache.buckets[key])
		}

		wg.Wait()
		close(ch)
	}()

	count := 0
	for length := range ch {
		count += length
	}

	return count
}

func (cache *GlobalCache) totalBucketsKeys() string {
	wg := &sync.WaitGroup{}
	ch := make(chan string)

	go func() {
		for key := range cache.buckets {
			wg.Add(1)

			go func(buck *bucket.Bucket) {
				ch <- buck.Keys()
				wg.Done()
			}(cache.buckets[key])
		}

		wg.Wait()
		close(ch)
	}()

	result := make([]string, 0)
	for msg := range ch {
		result = append(result, msg)
	}

	return strings.Join(result, ", ")
}

// SDBM hash function implemented in golang
// http://www.partow.net/programming/hashfunctions/index.html#SDBMHashFunction
func bucketHashFunc(numBuckets int) func(string) uint {
	return func(key string) uint {
		var hash uint64 = 0
		for _, char := range key {
			hash = uint64(char) + (hash << 6) + (hash << 16) - hash
		}
		return uint(hash % uint64(numBuckets))
	}
}
