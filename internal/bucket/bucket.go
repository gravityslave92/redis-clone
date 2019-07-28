package bucket

import (
	"strings"
	"sync"
	"time"
)

type Bucket struct {
	mu            sync.Mutex
	entries       map[string]*node
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

func (b *Bucket) Set(key, value string, expiration time.Duration) error {
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

func (b *Bucket) Get(key string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n, ok := b.entries[key]
	if !ok {
		return "", false
	}

	if n.ttl.Before(time.Now()) {
		// It has expired
		defer b.remove(key)
		return "", false
	}

	return n.value, true
}

func (b *Bucket) Keys() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	keys := make([]string, 0)
	for key, value := range b.entries {
		if value.ttl.Before(time.Now()) {
			b.remove(key)
		}
		keys = append(keys, key)
	}

	return strings.Join(keys, ", ")
}

func (b *Bucket) Remove(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.remove(key)
}

func (b *Bucket) remove(key string) bool {
	if _, ok := b.entries[key]; ok {
		delete(b.entries, key)

		return ok
	}

	return false
}

