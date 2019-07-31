package list_bucket

import (
	"errors"
	"github.com/spf13/cast"
	"strings"
	"sync"
	"time"
)

type ListBucket struct {
	mu      sync.Mutex
	entries map[string]*listNode
}

type listNode struct {
	value      string
	next, prev *listNode
	ttl        time.Time
}

var wrongArgsNum = errors.New("wrong number of arguments")

func NewBucket() *ListBucket {
	bucket := new(ListBucket)
	bucket.entries = make(map[string]*listNode)

	return bucket
}

func (b *ListBucket) Set(args ...string) error {
	if len(args) != 3 {
		return wrongArgsNum
	}

	key := args[0]
	value := args[1]
	expiration := cast.ToDuration(args[2])
	return b.set(key, value, expiration)
}

func (b *ListBucket) set(key, value string, expiration time.Duration) error {
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

// generic Get
func (b *ListBucket) Get(args ...string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}

	dictName := args[0]
	index := cast.ToInt(args[1])
	return b.get(dictName, index)
}

func (b *ListBucket) get(key string, indx int) (string, bool) {
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
	if indx == 0 && firstList.ttl.After(time.Now()) {
		return firstList.value, true
	}
	// key has only one list at all and it has expired
	if indx == 0 && firstList.ttl.Before(time.Now()) {
		b.removeWithoutLock(key, firstList.value)
		return "", false
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
		b.removeWithoutLock(key, listNeeded.value)
		return "", false
	}

	return listNeeded.value, true
}

//  len for interface impl
func (b *ListBucket) Len(args ...string) int {
	if len(args) != 1 {
		return -1
	}

	key := args[0]
	return b.len(key)
}

func (b *ListBucket) len(key string) int {
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

func (b *ListBucket) Keys(args ...string) string {
	if len(args) != 1 {
		return ""
	}

	key := args[0]
	return b.keys(key)
}

func (b *ListBucket) keys(key string) string {
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
			b.removeWithoutLock(key, list.value)
			continue
		}
		values = append(values, list.value)
	}

	return strings.Join(values, ", ")
}

func (b *ListBucket) Remove(args ...string) error {
	if len(args) != 2 {
		return wrongArgsNum
	}

	key := args[0]
	indx := cast.ToInt(args[1])
	return b.remove(key, indx)
}

func (b *ListBucket) remove(key string, indx int) error {
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

func (b *ListBucket) removeWithoutLock(key, value string) {
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
