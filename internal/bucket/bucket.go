package bucket

import (
	"errors"
	"fmt"
	"github.com/spf13/cast"
	"strings"
	"sync"
	"time"
)

type Bucket struct {
	mu      sync.Mutex
	entries map[string]*node
}

type node struct {
	key   string
	value string
	ttl   time.Time
}

func NewBucket() *Bucket {
	bucket := new(Bucket)
	bucket.entries = make(map[string]*node)

	return bucket
}

// generic Set
func (b *Bucket) Set(args ...string) error {
	if len(args) != 3 {
		return errors.New("wrong number of arguments")
	}

	key := args[0]
	value := args[1]
	expiration := cast.ToDuration(args[2])
	return b.set(key, value, expiration)
}

func (b *Bucket) set(key, value string, expiration time.Duration) error {
	// lock mutex on critical zone enter
	b.mu.Lock()
	defer b.mu.Unlock()

	newNode := new(node)
	newNode.key = key
	newNode.value = value
	newNode.ttl = time.Now().Add(expiration)

	if _, ok := b.entries[key]; ok {
		b.entries[key] = newNode

		return nil
	}
	// node does not exist
	b.entries[key] = newNode

	return nil
}

// generic get for interface implementation
func (b *Bucket) Get(args ...string) (string, bool) {
	key := args[0]
	return b.get(key)
}

func (b *Bucket) get(key string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n, ok := b.entries[key]
	if !ok {
		return "", false
	}

	if n.ttl.Before(time.Now()) {
		// It has expired
		defer b.removeWithoutLock(key)
		return "", false
	}

	return n.value, true
}

// generic keys
func (b *Bucket) Keys(args ...string) string {
	if len(args) != 0 {
		return ""
	}

	return b.keys()
}

func (b *Bucket) keys() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	keys := make([]string, 0)
	for key, value := range b.entries {
		if value.ttl.Before(time.Now()) {
			b.removeWithoutLock(key)
		}
		keys = append(keys, key)
	}

	return strings.Join(keys, ", ")
}

func (b *Bucket) Len(args ...string) int  {
	fmt.Println(args)
	fmt.Println(len(args))
	if len(args) != 0 {
		return  -1
	}
	
	return b.len()
}

func (b *Bucket) len() int  {
	return len(b.entries)
}

// generic remove
func (b *Bucket) Remove(args ...string) error {
	if len(args) != 1 {
		return errors.New("wrong arguments number")
	}

	key := args[0]
	return b.remove(key)
}

func (b *Bucket) remove(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.removeWithoutLock(key)
}

func (b *Bucket) removeWithoutLock(key string) error {
	if _, ok := b.entries[key]; ok {
		delete(b.entries, key)
		return nil
	}

	return errors.New("key not exists")
}
