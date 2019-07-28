package dict_bucket

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestDictBucket_Set(t *testing.T) {
	bucket := newBucket()
	{
		bucket.Set("test", "hello", "world", 10*time.Minute)
		assert.NotNil(t, bucket.entries["test"])
		assert.NotNil(t, bucket.entries["test"]["hello"])
		assert.EqualValues(t, bucket.entries["test"]["hello"].value, "world")
	}

	{
		t.Log("Given a second attempt it should renew ttl correctly")
		oldTTL := bucket.entries["test"]["hello"].ttl
		err := bucket.Set("test", "hello", "world", 100*time.Minute)
		assert.NoError(t, err)
		assert.True(t, oldTTL != bucket.entries["test"]["hello"].ttl)

	}
}

func TestDictBucket_Get(t *testing.T) {
	bucket := newBucket()
	testCases := setupTestCases(t, bucket)

	{
		t.Log("Given a dict it should return value by key")
		for _, testCase := range testCases {
			testCase := testCase // preserve variable copy within current closure
			t.Run(testCase.key, func(t *testing.T) {
				t.Parallel()

				value, ok := bucket.Get(testCase.dictKey, testCase.key)
				assert.True(t, ok)
				assert.EqualValues(t, testCase.value, value, fmt.Sprintf("dictBucket#Get returned %q, expectd %q", value, testCase.value))
			})
		}
	}

	{
		t.Log("It should not return value if key has expirad")
		bucket.Set("dict", "qwerty", "zxc", 50*time.Microsecond)
		<-time.After(51 * time.Microsecond)
		value, ok := bucket.Get("dict", "qwerty")
		assert.False(t, ok)
		assert.EqualValues(t, "", value)
	}
}

func TestDictBucket_Len(t *testing.T) {
	bucket := newBucket()
	setupTestCases(t, bucket)
	assert.EqualValues(t, bucket.Len("dict"), 5)

	{
		t.Log("Given an expired element it should return correct length")
		bucket.Set("dict", "i will expire", "soon", 50*time.Millisecond)
		assert.EqualValues(t, bucket.Len("dict"), 6)
		<-time.After(51 * time.Millisecond)
		assert.EqualValues(t, bucket.Len("dict"), 5)
	}
}

func TestDictBucket_Remove(t *testing.T) {
	bucket := newBucket()
	testCases := setupTestCases(t, bucket)

	{
		t.Log("Given a dict it should remove entry by dictKey and key")
		for _, testCase := range testCases {
			testCase := testCase // preserve variable copy within current closure
			t.Run(testCase.key, func(t *testing.T) {
				t.Parallel()

				err := bucket.Remove(testCase.dictKey, testCase.key)
				assert.NoError(t, err)
				value, ok := bucket.Get(testCase.dictKey, testCase.key)
				assert.False(t, ok)
				assert.EqualValues(t, value, "")
			})
		}
	}
}

func TestDictBucket_Keys(t *testing.T) {
	bucket := newBucket()
	testCases := setupTestCases(t, bucket)

	{
		keys := strings.Split(bucket.Keys("dict"), ", ")
		for _, testCase := range testCases {
			assert.Contains(t, keys, testCase.key)
		}
	}

	{
		expected := "i will expire"
		bucket.Set("dict", expected, "soon", 50*time.Millisecond)
		keys := strings.Split(bucket.Keys("dict"), ", ")

		assert.Contains(t, keys, expected)
		<-time.After(51 * time.Millisecond)
		keys = strings.Split(bucket.Keys("dict"), ", ")
		assert.NotContains(t, keys, expected)
	}
}

func setupTestCases(t *testing.T, bucket *dictBucket) []struct {
	dictKey  string
	key      string
	value    string
	duration time.Duration
} {
	testCases := []struct {
		dictKey  string
		key      string
		value    string
		duration time.Duration
	}{
		{"dict", "test", "cat", 10 * time.Minute},
		{"dict", "world", "moose", 5 * time.Minute},
		{"dict", "green", "red", 2 * time.Minute},
		{"dict", "sweet", "home", 4 * time.Minute},
		{"dict", "tomorrow", "evening", 33 * time.Minute},
	}

	for _, testCase := range testCases {
		err := bucket.Set(testCase.dictKey, testCase.key, testCase.value, testCase.duration)
		assert.NoError(t, err)
		assert.Contains(t, bucket.entries[testCase.dictKey], testCase.key, fmt.Sprintf("expected %q to be within dictionary entries map keys", testCase.key))
	}

	return testCases
}
