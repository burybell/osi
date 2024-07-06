package minio

import (
	"context"
	"errors"
	"github.com/burybell/osi"
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

type ObjectStore struct {
	config Config
	client *minio.Client
}

func NewObjectStore(config Config) (osi.ObjectStore, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.KeyID, config.Secret, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, err
	}
	return &ObjectStore{config: config, client: client}, nil
}

func MustNewObjectStore(config Config) osi.ObjectStore {
	store, err := NewObjectStore(config)
	if err != nil {
		panic(err)
	}
	return store
}

func (t *ObjectStore) Name() string {
	return Name
}

func (t *ObjectStore) Bucket(name string) osi.Bucket {
	return &bucket{
		config: t.config,
		client: t.client,
		bucket: name,
	}
}

func (t *ObjectStore) ACLEnum() osi.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config Config
	client *minio.Client
	bucket string
}

func (t *bucket) GetObject(ctx context.Context, path string) (osi.Object, error) {
	acl, err := t.client.GetObjectACL(ctx, t.bucket, path)
	if err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.Code == "NoSuchKey" {
			return nil, osi.ObjectNotFound
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

	object, err := t.client.GetObject(ctx, t.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return osi.NewObject(t.bucket, path, ACL, object), nil
}

func (t *bucket) PutObject(ctx context.Context, path string, reader io.Reader) error {
	return t.PutObjectWithACL(ctx, path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(ctx context.Context, path string, reader io.Reader, acl osi.ACL) error {
	opts := minio.PutObjectOptions{}
	opts.Header().Set("x-amz-acl", acl)
	opts.ContentType = mime.TypeByExtension(filepath.Ext(path))
	_, err := t.client.PutObject(ctx, t.bucket, path, reader, -1, opts)
	return err
}

func (t *bucket) HeadObject(ctx context.Context, path string) (bool, error) {
	_, err := t.client.StatObject(ctx, t.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(ctx context.Context, path string) error {
	return t.client.RemoveObject(ctx, t.bucket, path, minio.RemoveObjectOptions{})
}

func (t *bucket) ListObjects(ctx context.Context, prefix string) ([]osi.ObjectMeta, error) {
	var oms = make([]osi.ObjectMeta, 0)
	objects := t.client.ListObjects(ctx, t.bucket, minio.ListObjectsOptions{
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
		oms = append(oms, osi.NewObjectMeta(t.bucket, object.Key))
	}
	return oms, nil
}

func (t *bucket) GetObjectSize(ctx context.Context, path string) (osi.Size, error) {
	stat, err := t.client.StatObject(ctx, t.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return osi.NewSize(stat.Size), nil
}

func (t *bucket) SignURL(ctx context.Context, path string, method string, expiredInDur time.Duration) (string, error) {
	url, err := t.client.Presign(ctx, method, t.bucket, path, expiredInDur, nil)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

func (t *bucket) DeleteObjects(ctx context.Context, paths []string) error {
	var batchSize = 999
	if len(paths) < batchSize {
		return t.deleteFiles(ctx, paths)
	}
	for i := 0; i < len(paths); i += batchSize {
		edge := i + batchSize
		if len(paths) < edge {
			edge = len(paths)
		}
		err := t.deleteFiles(ctx, paths[i:edge])
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *bucket) deleteFiles(ctx context.Context, paths []string) error {
	objects := make(chan minio.ObjectInfo)
	go func() {
		for i := range paths {
			objects <- minio.ObjectInfo{Key: paths[i]}
		}
	}()
	errCh := t.client.RemoveObjects(ctx, t.bucket, objects, minio.RemoveObjectsOptions{})
	for ch := range errCh {
		if ch.Err != nil {
			return ch.Err
		}
	}
	return nil
}

type aclEnum struct {
}

func (t aclEnum) Private() osi.ACL {
	return "private"
}

func (t aclEnum) PublicRead() osi.ACL {
	return "public-read"
}

func (t aclEnum) PublicReadWrite() osi.ACL {
	return "public-read-write"
}

func (t aclEnum) Default() osi.ACL {
	return ""
}
