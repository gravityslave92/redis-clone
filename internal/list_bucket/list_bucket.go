package list_bucket

import (
	"errors"
	"strings"
	"sync"
	"time"
)

type listBucket struct {
	mu      sync.Mutex
	entries map[string]*listNode
}

type listNode struct {
	value      string
	next, prev *listNode
	ttl        time.Time
}

func NewListBucket() *listBucket {
	bucket := new(listBucket)
	bucket.entries = make(map[string]*listNode)

	return bucket
}

func (b *listBucket) Set(key, value string, expiration time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	node := new(listNode)
	node.value = value
	node.ttl = time.Now().Add(expiration)

	firstList, ok := b.entries[key]
	if !ok {
		// key does not exists as well as listNode
		b.entries[key] = node

		return nil
	}
	if firstList.next == nil {
		firstList.next = node

		return nil
	}

	for list := firstList.next; list != nil; list = list.next {
		if list.value == value {
			list.ttl = node.ttl

			break
		}
		// node does not exist
		if list.next == nil {
			list.next = node
			node.prev = list
		}
	}

	return nil
}

func (b *listBucket) Get(key string, indx int) (string, bool) {
	listLen := b.Len(key)
	if listLen == -1 {
		return "", false
	}

	if listLen <= indx {
		return "", false
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// key has only one list at all
	firstList := b.entries[key]
	if indx == 0 {
		return firstList.value, true
	}

	count := 0
	var listNeeded *listNode

	for list := firstList.next; list != nil; list = list.next {
		count++
		if count == indx {
			listNeeded = list

			break
		}
	}

	if listNeeded.ttl.Before(time.Now()) {
		b.remove(key, listNeeded.value)
		return "", false
	}

	return listNeeded.value, true
}

func (b *listBucket) Len(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	firstList, ok := b.entries[key]
	if !ok {
		return -1
	}

	count := 1
	for list := firstList.next; list != nil; list = list.next {
		count++
	}

	return count
}

func (b *listBucket) List(key string) string {
	listLen := b.Len(key)
	if listLen == -1 {
		return ""
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	values := make([]string, 0)

	firstList := b.entries[key]
	values = append(values, firstList.value)
	for list := firstList.next; list != nil; list = list.next {
		if list.ttl.Before(time.Now()) {
			b.remove(key, list.value)
			continue
		}
		values = append(values, list.value)
	}

	return strings.Join(values, ", ")
}

func (b *listBucket) Remove(key string, indx int) error {
	listLen := b.Len(key)
	if listLen == -1 {
		return errors.New("key does not exits or has been deleted already")
	}

	if indx >= listLen {
		return errors.New("index out of range")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if listLen == 1 && indx == 0 {
		delete(b.entries, key)

		return nil
	}

	count := 0
	firstList := b.entries[key]
	for list := firstList.next; list != nil; list = list.next {
		count++
		if count != indx {
			continue
		}

		if list.next == nil {
			list.prev = nil

			return nil
		}

		list.next.prev = list.prev
	}

	return nil
}

func (b *listBucket) remove(key, value string) {
	firstList := b.entries[key]
	if firstList.value == value {
		firstList.next.prev = nil
		b.entries[key] = firstList.next

		return
	}

	for list := firstList.next; list != nil; list = list.next {
		if list.value != value {
			continue
		}

		if list.next == nil {
			list.prev.next = nil

			return
		}

		list.next.prev = list.prev
	}
}
