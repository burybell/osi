package minio_test

import (
	"encoding/json"
	"github.com/burybell/oss"
	"github.com/burybell/oss/minio"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"strings"
	"testing"
)

var (
	objectStore oss.ObjectStore
	bucket      oss.Bucket
)

type Config struct {
	Minio           minio.Config `json:"minio"`
	MinioBucketName string       `json:"minio_bucket_name"`
}

func init() {
	f, err := os.Open("../config.json")
	if err != nil {
		panic(err)
	}

	var config Config
	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		panic(err)
	}
	objectStore = minio.MustNewObjectStore(config.Minio)
	bucket = objectStore.Bucket(config.MinioBucketName)
}

func TestBucket_PutObject(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	object, err := bucket.GetObject("test/example.txt")
	assert.NoError(t, err)
	assert.Equal(t, ".txt", object.Extension())
	assert.Equal(t, "test/example.txt", object.ObjectPath())
	bs, err := io.ReadAll(object)
	assert.NoError(t, err)
	assert.Equal(t, "some text", string(bs))
}

func TestBucket_DeleteObject(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	err = bucket.DeleteObject("test/example.txt")
	assert.NoError(t, err)
	_, err = bucket.GetObject("test/example.txt")
	assert.ErrorIs(t, err, oss.ObjectNotFound)
}

func TestBucket_GetObject(t *testing.T) {
	TestBucket_PutObject(t)
}

func TestBucket_GetObjectSize(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	size, err := bucket.GetObjectSize("test/example.txt")
	assert.NoError(t, err)
	assert.Equal(t, int64(9), size.Size())
}

func TestBucket_HeadObject(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	exist, err := bucket.HeadObject("test/example.txt")
	assert.NoError(t, err)
	assert.True(t, exist)
}

func TestBucket_ListObject(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	objects, err := bucket.ListObject("test/")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(objects))
}
