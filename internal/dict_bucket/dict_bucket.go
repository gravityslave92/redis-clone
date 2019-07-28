package dict_bucket

import (
	"errors"
	"github.com/spf13/cast"
	"strings"
	"sync"
	"time"
)

var (
	dictionaryNotExists = errors.New("dictionary does not exist")
	keyNotFound         = errors.New("key not found")
	keyExpired          = errors.New("key has expired")
)

type DictBucket struct {
	mu      sync.Mutex
	entries map[string]map[string]*dictNode
}

type dictNode struct {
	value string
	ttl   time.Time
}

func NewBucket() *DictBucket {
	bucket := new(DictBucket)
	bucket.entries = make(map[string]map[string]*dictNode)
	return bucket
}

func (b *DictBucket) Set(args ...string) error {
	if len(args) != 4 {
		return errors.New("wrong arguments number")
	}

	dictName := args[0]
	key := args[1]
	value := args[2]
	expiration := cast.ToDuration(args[3])
	return b.set(dictName, key, value, expiration)
}

func (b *DictBucket) set(dictName, key, value string, expiration time.Duration) error {
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
		err := b.removeWithoutLock(dictName, key)
		if err != nil {
			return err
		}

		return keyExpired
	}

	hash[key] = node
	return nil
}

func (b *DictBucket) Get(args ...string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}

	dictName := args[0]
	key := args[1]
	return b.get(dictName, key)
}

func (b *DictBucket) get(dictName, key string) (string, bool) {
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
		b.removeWithoutLock(dictName, key)
		return "", false
	}

	return dictNode.value, true
}

func (b *DictBucket) Len(args ...string) int {
	if len(args) != 1 {
		return -1
	}

	dictName := args[0]
	return b.len(dictName)
}

func (b *DictBucket) len(dictName string) int {
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

		b.removeWithoutLock(dictName, key)
	}

	return count
}

func (b *DictBucket) Keys(args ...string) string {
	if len(args) != 1 {
		return ""
	}

	dictName := args[0]
	return b.keys(dictName)
}

func (b *DictBucket) keys(dictName string) string {
	dictLen := b.Len(dictName)
	if dictLen == -1 {
		return ""
	}

	keys := make([]string, 0)
	for key, value := range b.entries[dictName] {
		if value.ttl.Before(time.Now()) {
			b.removeWithoutLock(dictName, key)
		}

		keys = append(keys, key)
	}

	return strings.Join(keys, ", ")
}

func (b *DictBucket) Remove(args ...string) error {
	if len(args) != 2 {
		return errors.New("wrong arguments number")
	}

	dictName := args[0]
	key := args[1]
	return b.remove(dictName, key)
}

func (b *DictBucket) remove(dictName, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.removeWithoutLock(dictName, key)
}

func (b *DictBucket) removeWithoutLock(dictName, key string) error {
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
