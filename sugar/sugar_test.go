package sugar_test

import (
	"encoding/json"
	"fmt"
	"github.com/burybell/oss"
	"github.com/burybell/oss/aliyun"
	"github.com/burybell/oss/huawei"
	"github.com/burybell/oss/local"
	"github.com/burybell/oss/minio"
	"github.com/burybell/oss/s3"
	"github.com/burybell/oss/sugar"
	"github.com/burybell/oss/tencent"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	objectstore oss.ObjectStore
	bucket      oss.Bucket
)

type Config struct {
	AliYun            aliyun.Config  `json:"aliyun"`
	S3                s3.Config      `json:"s3"`
	Tencent           tencent.Config `json:"tencent"`
	Local             local.Config   `json:"local"`
	Minio             minio.Config   `json:"minio"`
	Huawei            huawei.Config  `json:"huawei"`
	UseName           string         `json:"use_name"`
	AliYunBucketName  string         `json:"aliyun_bucket_name"`
	S3BucketName      string         `json:"s3_bucket_name"`
	TencentBucketName string         `json:"tencent_bucket_name"`
	LocalBucketName   string         `json:"local_bucket_name"`
	MinioBucketName   string         `json:"minio_bucket_name"`
	HuaweiBucketName  string         `json:"huawei_bucket_name"`
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
	switch config.UseName {
	case aliyun.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseAliYun(config.AliYun))
		bucket = objectstore.Bucket(config.AliYunBucketName)
	case s3.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseS3(config.S3))
		bucket = objectstore.Bucket(config.S3BucketName)
	case tencent.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseTencent(config.Tencent))
		bucket = objectstore.Bucket(config.TencentBucketName)
	case local.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseLocal(config.Local))
		bucket = objectstore.Bucket(config.LocalBucketName)
	case minio.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseMinio(config.Minio))
		bucket = objectstore.Bucket(config.MinioBucketName)
	case huawei.Name:
		objectstore = sugar.MustNewObjectStore(sugar.UseHuawei(config.Huawei))
		bucket = objectstore.Bucket(config.HuaweiBucketName)
	default:
		panic("no support objectstore")
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

func Test_bucket_SignURL(t *testing.T) {
	err := bucket.PutObject("test/example.txt", strings.NewReader("some text"))
	assert.NoError(t, err)
	url, err := bucket.SignURL("test/example.txt", http.MethodGet, time.Second*100)
	assert.NoError(t, err)
	fmt.Println(url)
}
