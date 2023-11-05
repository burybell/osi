package sugar_test

import (
	"encoding/json"
	"fmt"
	"github.com/burybell/osi"
	"github.com/burybell/osi/cos"
	"github.com/burybell/osi/local"
	"github.com/burybell/osi/minio"
	"github.com/burybell/osi/obs"
	"github.com/burybell/osi/oss"
	"github.com/burybell/osi/s3"
	"github.com/burybell/osi/sugar"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	objectStore osi.ObjectStore
	bucket      osi.Bucket
)

type Config struct {
	OSS               oss.Config   `json:"oss"`
	S3                s3.Config    `json:"s3"`
	COS               cos.Config   `json:"cos"`
	Local             local.Config `json:"local"`
	Minio             minio.Config `json:"minio"`
	OBS               obs.Config   `json:"obs"`
	UseName           string       `json:"use_name"`
	AliYunBucketName  string       `json:"oss_bucket_name"`
	S3BucketName      string       `json:"s3_bucket_name"`
	TencentBucketName string       `json:"cos_bucket_name"`
	LocalBucketName   string       `json:"local_bucket_name"`
	MinioBucketName   string       `json:"minio_bucket_name"`
	HuaweiBucketName  string       `json:"obs_bucket_name"`
}

func init() {
	configFile := "../config.json"
	var config Config
	f, err := os.Open(configFile)
	if err != nil {
		f, err = os.OpenFile(configFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		config = Config{
			Local:           local.Config{BasePath: "/tmp/osi", HttpAddr: "http://localhost:8080", HttpSecret: "example"},
			UseName:         local.Name,
			LocalBucketName: "example",
		}
		err = json.NewEncoder(f).Encode(config)
		if err != nil {
			panic(err)
		}
		_ = f.Close()
	} else {
		err = json.NewDecoder(f).Decode(&config)
		if err != nil {
			panic(err)
		}
		_ = f.Close()
	}

	switch config.UseName {
	case oss.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseOSS(config.OSS))
		bucket = objectStore.Bucket(config.AliYunBucketName)
	case s3.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseS3(config.S3))
		bucket = objectStore.Bucket(config.S3BucketName)
	case cos.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseCOS(config.COS))
		bucket = objectStore.Bucket(config.TencentBucketName)
	case local.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseLocal(config.Local))
		bucket = objectStore.Bucket(config.LocalBucketName)
	case minio.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseMinio(config.Minio))
		bucket = objectStore.Bucket(config.MinioBucketName)
	case obs.Name:
		objectStore = sugar.MustNewObjectStore(sugar.UseOBS(config.OBS))
		bucket = objectStore.Bucket(config.HuaweiBucketName)
	default:
		panic("no support store")
	}
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
	assert.ErrorIs(t, err, osi.ObjectNotFound)
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

func Test_bucket_SignURL(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	url, err := bucket.SignURL("test/example.txt", http.MethodGet, time.Second*100)
	assert.NoError(t, err)
	fmt.Println(url)
}
