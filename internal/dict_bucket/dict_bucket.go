package dict_bucket

import (
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	dictionaryNotExists = errors.New("dictionary does not exist")
	keyNotFound         = errors.New("key not found")
	keyExpired          = errors.New("key has expired")
)

type dictBucket struct {
	mu      sync.Mutex
	entries map[string]map[string]*dictNode

	bucketSize    uint64
	maxBucketSize uint64
	dictBaseSize  uint64
}

type dictNode struct {
	value string
	ttl   time.Time
	size  uint64
}

func newBucket() *dictBucket {
	bucket := new(dictBucket)
	bucket.entries = make(map[string]map[string]*dictNode)
	return bucket
}

func (b *dictBucket) Set(dictName, key, value string, expiration time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	node := new(dictNode)
	node.value = value
	node.ttl = time.Now().Add(expiration)
	// dictionary name check
	hash, ok := b.entries[dictName]
	if !ok {
		//dict does not exists
		b.entries[dictName] = make(map[string]*dictNode)
		b.entries[dictName][key] = node

		return nil
	}

	// check if key exists
	dictNode, ok := hash[key]
	if !ok {
		hash[key] = node
		return nil
	}
	// check expiration
	if dictNode.ttl.Before(time.Now()) {
		err := b.remove(dictName, key)
		if err != nil {
			return err
		}

		return keyExpired
	}

	hash[key] = node
	return nil
}

func (b *dictBucket) Get(dictName, key string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// dictionary check
	dict, ok := b.entries[dictName]
	if !ok {
		return "", false
	}
	// key check
	dictNode, ok := dict[key]
	if !ok {
		return "", false
	}

	if dictNode.ttl.Before(time.Now()) {
		// @tod remove
		return "", false
	}

	return dictNode.value, true
}

func (b *dictBucket) Len(dictName string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	dict, ok := b.entries[dictName]
	if !ok {
		return -1
	}

	count := 0
	for key, value := range dict {
		if value.ttl.After(time.Now()) {
			count++
			continue
		}

		b.remove(dictName, key)
	}

	return count
}

func (b *dictBucket) Keys(dictName string) string {
	dictLen := b.Len(dictName)
	if dictLen == -1 {
		return ""
	}

	keys := make([]string, 0)
	for key, value := range b.entries[dictName] {
		if value.ttl.Before(time.Now()) {
			b.remove(dictName, key)
		}

		keys = append(keys, key)
	}

	return strings.Join(keys, ", ")
}

func (b *dictBucket) Remove(dictName, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.remove(dictName, key)
}

func (b *dictBucket) remove(dictName, key string) error {
	dict, ok := b.entries[dictName]
	if !ok {
		return dictionaryNotExists
	}
	// delete whole dictionary if keys is empty
	if key == "" {
		delete(b.entries, dictName)
		return nil
	}

	if _, ok := dict[key]; !ok {
		return keyNotFound
	}

	delete(dict, key)
	// delete whole dictionary if entry is the last entry
	if len(dict) == 0 {
		delete(b.entries, dictName)
	}

	return nil
}
