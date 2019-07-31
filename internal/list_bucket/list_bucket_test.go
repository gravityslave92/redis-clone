package list_bucket

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestListBucket_Set(t *testing.T) {
	bucket := NewBucket()
	err := bucket.Set("hello", "world", "10m")

	{
		t.Log("Given a successful insert it should set node correctly")
		list := bucket.entries["hello"]
		assert.NoError(t, err)
		assert.EqualValues(t, list.value, "world")
		assert.Nil(t, list.next)
	}

	{
		t.Log("It should append second node correctly ")
		err := bucket.Set("hello", "new world", "10m")
		assert.NoError(t, err)

		list := bucket.entries["hello"]
		nextList := list.next

		assert.Equal(t, list.next, nextList)
		assert.EqualValues(t, nextList.value, "new world")
	}

	{
		t.Log("It should update TTL correctly")
		oldTTL := bucket.entries["hello"].next.ttl

		err := bucket.Set("hello", "new world", "200m")
		assert.NoError(t, err)
		assert.True(t, oldTTL != bucket.entries["hello"].next.ttl)
	}

}

func TestListBucket_Get(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)

	{
		t.Log("Given a list it should return value by index")
		values := bucket.Keys("test")
		for index, testCase := range testCases {
			testCase := testCase // preserve variable copy within current closure
			index := string(index)
			t.Run(testCase.value, func(t *testing.T) {
				t.Parallel()
				value, ok := bucket.Get(testCase.key, index)

				assert.True(t, ok)
				assert.Contains(t, values, value)
			})
		}
	}

	{
		t.Log("It should not return value if key has expired")
		bucket.Set("test", "qwerty", "50ms")
		<-time.After(51 * time.Millisecond)
		_, ok := bucket.Get("test", "5")
		assert.False(t, ok)
	}
}

func TestListBucket_List(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)

	{
		t.Log("It should return all values")
		values := strings.Split(bucket.Keys("test"), ", ")
		for _, testCase := range testCases {
			assert.Contains(t, values, testCase.value)
		}
	}

	{
		t.Log("Given a non existing  list it should return empty string")
		got := bucket.Keys("not exists")
		assert.Empty(t, got)
	}
}

func TestListBucket_Len(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)

	{
		t.Log("Given an existing list it should return proper length")
		expected := len(testCases)
		got := bucket.Len("test")
		assert.EqualValues(t, expected, got, fmt.Sprintf("expected Len(%q) to be %d, go %d", "test", expected, got))
	}

	{
		t.Log("Given nonexsisted list name it should return -1")
		expected := -1
		got := bucket.Len("not exists")
		assert.EqualValues(t, expected, got, fmt.Sprintf("expected Len(%q) to be %d, got %d", "not exists", got, expected))

	}
}

func TestListBucket_Remove(t *testing.T) {
	bucket := NewBucket()
	testCases := setupTestCases(t, bucket)

	for index, testCase := range testCases {
		testCase := testCase
		index := string(index)
		t.Run(testCase.value, func(t *testing.T) {
			t.Parallel()

			err := bucket.Remove(testCase.key, index)
			assert.NoError(t, err)
		})
	}
}

func setupTestCases(t *testing.T, bucket *ListBucket) []struct {
	key, value, duration string
} {
	testCases := []struct {
		key, value, duration string
	}{
		{"test", "cat", "10m"},
		{"test", "moose", "5m"},
		{"test", "red", "2m"},
		{"test", "home", "4m"},
		{"test", "evening", "30m"},
	}

	for _, testCase := range testCases {
		err := bucket.Set(testCase.key, testCase.value, testCase.duration)
		assert.NoError(t, err)

		for list := bucket.entries[testCase.key]; list != nil; list = list.next {
			if list.next != nil {
				continue
			}
			assert.EqualValues(t, testCase.value, list.value)
		}
	}

	return testCases
}
