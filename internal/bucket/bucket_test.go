package bucket

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestBucket_Set(t *testing.T) {
	buck := NewBucket()
	err := buck.Set("hello", "world", "10m")
	assert.NoError(t, err)

	{
		t.Log("Given a bucket with 0 nodes it should append  the first node")
		assert.Contains(t, buck.entries, "hello", fmt.Sprintf("expected %q to be within entries map keys", "hello"))
	}

	{
		t.Log("Given a bucket with at least one node it should append  node after calling bucket#Set() func")
		buck.Set("world", "hello", "10m")
		assert.Contains(t, buck.entries, "world", fmt.Sprintf("expected %q to be within entries map keys", "world"))

	}

	{
		t.Log("Given an entry within a bucket it should update TTL and value on second  bucket#Set() call")
		oldTTL := buck.entries["hello"].ttl
		oldValue := buck.entries["hello"].value

		err := buck.Set("hello", "new world", "200m")
		assert.NoError(t, err)
		new := buck.entries["hello"]

		assert.NotEqual(t, new.ttl, oldTTL, fmt.Sprintf("expected ttl to be %v, got %v", new.ttl, oldTTL))
		assert.NotEqual(t, new.value, oldValue, fmt.Sprintf("expected value to be %q, got %q", new.value, oldValue))
	}
}

func TestBucket_Get(t *testing.T) {
	buck := NewBucket()
	testCases := setupTestCases(t, buck)

	for _, testCase := range testCases {
		testCase := testCase // preserve variable copy within current closure
		t.Run(testCase.key, func(t *testing.T) {
			t.Parallel()
			value, ok := buck.Get(testCase.key)

			assert.True(t, ok)
			assert.EqualValues(t, value, testCase.value)
		})
	}

	{
		t.Log("It should not return  value if key has expired")
		buck.Set("testt", "tac", "50ms")
		<-time.After(51 * time.Millisecond)
		_, ok := buck.Get("testt")
		assert.False(t, ok)
	}
}

func TestBucket_Keys(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)
	keys := bucket.Keys()

	assert.EqualValues(t, len(strings.Split(keys, ", ")), len(testCases))
	for _, testCase := range testCases {
		assert.Containsf(t, keys, testCase.key, "error message %s", "formatted")
	}
}

func TestBucket_Remove(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.key, func(t *testing.T) {
			t.Parallel()
			ok := bucket.Remove(testCase.key)

			assert.NoError(t, ok)
			assert.NotContains(t, bucket.entries, testCase.key)
		})
	}
}

func TestBucket_Len(t *testing.T) {
	bucket := NewBucket()
	assert.Zero(t, bucket.Len())
	assert.EqualValues(t, bucket.Len("test"), -1.)
}

func setupTestCases(t *testing.T, bucket *Bucket) []struct {
	key      string
	value    string
	duration string
} {
	testCases := []struct {
		key      string
		value    string
		duration string
	}{
		{"test", "cat", "10m"},
		{"world", "moose", "5m"},
		{"green", "red", "2m"},
		{"sweet", "home", "4m"},
		{"tomorrow", "evening", "30m"},
	}

	for _, testCase := range testCases {
		err := bucket.Set(testCase.key, testCase.value, testCase.duration)
		assert.NoError(t, err)
		assert.Contains(t, bucket.entries, testCase.key, fmt.Sprintf("expected %q to be within entries map keys", testCase.key))
	}

	return testCases
}
