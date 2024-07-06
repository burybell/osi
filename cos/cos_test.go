package cos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/burybell/osi"
	"github.com/burybell/osi/cos"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	ctx         = context.Background()
	objectStore osi.ObjectStore
	bucket      osi.Bucket
)

type Config struct {
	COS           cos.Config `json:"cos"`
	COSBucketName string     `json:"cos_bucket_name"`
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
	objectStore = cos.MustNewObjectStore(config.COS)
	bucket = objectStore.Bucket(config.COSBucketName)
}

func TestBucket_PutObject(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	object, err := bucket.GetObject(ctx, "test/example.txt")
	assert.NoError(t, err)
	assert.Equal(t, ".txt", object.Extension())
	assert.Equal(t, "test/example.txt", object.ObjectPath())
	bs, err := io.ReadAll(object)
	assert.NoError(t, err)
	assert.Equal(t, "some text", string(bs))
}

func TestBucket_DeleteObject(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	err = bucket.DeleteObject(ctx, "test/example.txt")
	assert.NoError(t, err)
	_, err = bucket.GetObject(ctx, "test/example.txt")
	assert.ErrorIs(t, err, osi.ObjectNotFound)
}

func TestBucket_GetObject(t *testing.T) {
	TestBucket_PutObject(t)
}

func TestBucket_GetObjectSize(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	size, err := bucket.GetObjectSize(ctx, "test/example.txt")
	assert.NoError(t, err)
	assert.Equal(t, int64(9), size.Size())
}

func TestBucket_HeadObject(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	exist, err := bucket.HeadObject(ctx, "test/example.txt")
	assert.NoError(t, err)
	assert.True(t, exist)
}

func TestBucket_ListObject(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	objects, err := bucket.ListObjects(ctx, "test/")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(objects))
}

func Test_bucket_SignURL(t *testing.T) {
	err := bucket.PutObject(ctx, "test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	url, err := bucket.SignURL(ctx, "test/example.txt", http.MethodGet, time.Second*100)
	assert.NoError(t, err)
	t.Logf("presigned url: %s", url)
}

func Test_bucket_DeleteObjects(t *testing.T) {
	paths := make([]string, 0)
	for i := 0; i < 10; i++ {
		filepath := fmt.Sprintf("test/example.txt")
		paths = append(paths, filepath)
		err := bucket.PutObject(ctx, filepath, strings.NewReader("some text"))
		assert.NoError(t, err)
	}
	err := bucket.DeleteObjects(ctx, paths)
	assert.NoError(t, err)

	exist, err := bucket.HeadObject(ctx, paths[0])
	assert.NoError(t, err)
	assert.False(t, exist)
}
