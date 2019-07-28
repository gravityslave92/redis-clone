package global_cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGlobalCache_ProcessCommand_keyValue(t *testing.T) {
	cache := NewCache(32)

	{
		reply := cache.ProcessCommand([]string{"SET", "testArg", "hello world", "100m"})
		assert.EqualValues(t, "Success\n", reply)
	}

	{
		reply := cache.ProcessCommand([]string{"GET", "testArg"})
		assert.EqualValues(t, "hello world\n", reply)
	}

	{
		cache.ProcessCommand([]string{"SET", "testArg1", "hello", "100m"})
		reply := cache.ProcessCommand([]string{"LEN"})
		assert.EqualValues(t, reply, "2\n")
		reply = cache.ProcessCommand([]string{"KEYS"})
		assert.Contains(t, reply, "testArg")
		assert.Contains(t, reply, "testArg1")
	}

	{
		cache.ProcessCommand([]string{"REM", "testArg1"})
		reply := cache.ProcessCommand([]string{"GET", "testArg1"})
		assert.EqualValues(t, reply, "value does not exist for given arguments\n")
	}
}

func TestGlobalCache_ProcessCommand_lists(t *testing.T) {
	cache := NewCache(32)
	{
		reply := cache.ProcessCommand([]string{"ZSET", "testArr", "hello world", "100m"})
		assert.EqualValues(t, "Success\n", reply)
	}

	{
		cache.ProcessCommand([]string{"ZSET", "testArr", "world", "1h"})
		cache.ProcessCommand([]string{"ZSET", "testArr", "yesterday", "1h"})
		reply := cache.ProcessCommand([]string{"ZGET", "testArr", "2"})
		assert.EqualValues(t, "yesterday\n", reply)
	}

	{
		reply := cache.ProcessCommand([]string{"ZLEN", "testArr"})
		assert.EqualValues(t, reply, "3\n")
		reply = cache.ProcessCommand([]string{"ZKEYS", "testArr"})
		assert.Contains(t, reply, "hello world")
		assert.Contains(t, reply, "world")
		assert.Contains(t, reply, "yesterday")
	}
}

func TestGlobalCache_ProcessCommand_dictionary(t *testing.T) {
	cache := NewCache(32)
	{
		reply := cache.ProcessCommand([]string{"DSET", "testDict", "random key", "hello world", "100m"})
		assert.EqualValues(t, "Success\n", reply)
	}

	{
		cache.ProcessCommand([]string{"DSET", "testDict", "world", "yesterday", "1h"})
		cache.ProcessCommand([]string{"DSET", "testDict", "tomorrow", "red", "1h"})
		reply := cache.ProcessCommand([]string{"DGET", "testDict", "world"})
		assert.EqualValues(t, "yesterday\n", reply)
	}

	{
		reply := cache.ProcessCommand([]string{"DLEN", "testDict"})
		assert.EqualValues(t, reply, "3\n")
		reply = cache.ProcessCommand([]string{"DKEYS", "testDict"})
		assert.Contains(t, reply, "random key")
		assert.Contains(t, reply, "world")
		assert.Contains(t, reply, "tomorrow")
	}

	{
		cache.ProcessCommand([]string{"DREM", "testDict", "random key"})
		reply := cache.ProcessCommand([]string{"DGET", "testDict", "random key"})
		assert.EqualValues(t, "value does not exist for given arguments\n", reply)
	}
}
