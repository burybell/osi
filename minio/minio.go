package minio

import (
	"context"
	"errors"
	"github.com/burybell/oss"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"mime"
	"path/filepath"
	"strings"
	"time"
)

const (
	Name = "minio"
)

type Config struct {
	Region   string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID    string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret   string `yaml:"secret" mapstructure:"secret" json:"secret"`
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint" json:"endpoint"`
	UseSSL   bool   `yaml:"use_ssl" mapstructure:"use_ssl" json:"use_ssl"`
}

type objectstore struct {
	config Config
	client *minio.Client
}

func NewObjectStore(config Config) (oss.ObjectStore, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.KeyID, config.Secret, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, err
	}
	return &objectstore{config: config, client: client}, nil
}

func MustNewObjectStore(config Config) oss.ObjectStore {
	store, err := NewObjectStore(config)
	if err != nil {
		panic(err)
	}
	return store
}

func (t *objectstore) Name() string {
	return Name
}

func (t *objectstore) Bucket(name string) oss.Bucket {
	return &bucket{
		config: t.config,
		client: t.client,
		bucket: name,
	}
}

func (t *objectstore) ACLEnum() oss.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config Config
	client *minio.Client
	bucket string
}

func (t *bucket) GetObject(path string) (oss.Object, error) {
	acl, err := t.client.GetObjectACL(context.TODO(), t.bucket, path)
	if err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.Code == "NoSuchKey" {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}

	var publicACL = make(map[string]int)
	for _, grant := range acl.Grant {
		publicACL[grant.Permission] = 1
	}

	var ACL = ""
	if publicACL["FULL_CONTROL"] == 1 || (publicACL["READ"] == 1 && publicACL["WRITE"] == 1) {
		ACL = "public-read-write"
	} else if publicACL["READ"] == 1 {
		ACL = "public-read"
	} else {
		ACL = "private"
	}

	object, err := t.client.GetObject(context.TODO(), t.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return oss.NewObject(t.bucket, path, ACL, object), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl oss.ACL) error {
	opts := minio.PutObjectOptions{}
	opts.Header().Set("x-amz-acl", acl)
	opts.ContentType = mime.TypeByExtension(filepath.Ext(path))
	_, err := t.client.PutObject(context.TODO(), t.bucket, path, reader, -1, opts)
	return err
}

func (t *bucket) HeadObject(path string) (bool, error) {
	_, err := t.client.StatObject(context.TODO(), t.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(path string) error {
	return t.client.RemoveObject(context.TODO(), t.bucket, path, minio.RemoveObjectOptions{})
}

func (t *bucket) ListObject(prefix string) ([]oss.ObjectMeta, error) {
	var oms = make([]oss.ObjectMeta, 0)
	objects := t.client.ListObjects(context.TODO(), t.bucket, minio.ListObjectsOptions{
		Prefix:  prefix,
		MaxKeys: 200,
	})

	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}
		if strings.HasSuffix(object.Key, "/") {
			continue
		}
		oms = append(oms, oss.NewObjectMeta(t.bucket, object.Key))
	}
	return oms, nil
}

func (t *bucket) GetObjectSize(path string) (oss.Size, error) {
	stat, err := t.client.StatObject(context.TODO(), t.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return oss.NewSize(stat.Size), nil
}

func (t *bucket) SignURL(path string, method string, expiredInDur time.Duration) (string, error) {
	url, err := t.client.Presign(context.TODO(), method, t.bucket, path, expiredInDur, nil)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

type aclEnum struct {
}

func (t aclEnum) Private() oss.ACL {
	return "private"
}

func (t aclEnum) PublicRead() oss.ACL {
	return "public-read"
}

func (t aclEnum) PublicReadWrite() oss.ACL {
	return "public-read-write"
}

func (t aclEnum) Default() oss.ACL {
	return ""
}
