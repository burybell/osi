package aliyun

import (
	"errors"
	"fmt"
	aliyun "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/burybell/oss"
	"io"
	"strconv"
	"strings"
)

const (
	Name = "aliyun"
)

type Config struct {
	Region   string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID    string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret   string `yaml:"secret" mapstructure:"secret" json:"secret"`
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint" json:"endpoint"`
}

type objectstore struct {
	config Config
	client *aliyun.Client
}

func NewObjectStore(config Config) (oss.ObjectStore, error) {
	if config.Endpoint == "" {
		config.Endpoint = fmt.Sprintf("https://oss-%s.aliyuncs.com", config.Region)
	}
	client, err := aliyun.New(config.Endpoint, config.KeyID, config.Secret)
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
	return &bucket{config: t.config, client: t.client, bucket: name}
}

func (t *objectstore) ACLEnum() oss.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config Config
	client *aliyun.Client
	bucket string
}

func (t *bucket) GetObject(path string) (oss.Object, error) {
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return nil, err
	}
	acl, err := bkt.GetObjectACL(path)
	if err != nil {
		var serverError aliyun.ServiceError
		if errors.As(err, &serverError) && serverError.Code == "NoSuchKey" {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	object, err := bkt.GetObject(path)
	if err != nil {
		return nil, err
	}
	return oss.NewObject(t.bucket, path, acl.ACL, object), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl oss.ACL) error {
	object := oss.NewObject(t.bucket, path, acl, io.NopCloser(reader))
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return err
	}
	return bkt.PutObject(object.ObjectPath(), object, aliyun.ObjectACL(aliyun.ACLType(acl)))
}

func (t *bucket) HeadObject(path string) (bool, error) {
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return false, err
	}
	return bkt.IsObjectExist(path)
}

func (t *bucket) DeleteObject(path string) error {
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return err
	}
	return bkt.DeleteObject(path)
}

func (t *bucket) ListObject(prefix string) ([]oss.ObjectMeta, error) {
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return nil, err
	}

	var oms = make([]oss.ObjectMeta, 0)
	var marker = ""
	for {
		objects, err := bkt.ListObjects(aliyun.Prefix(prefix), aliyun.MaxKeys(200), aliyun.Marker(marker))
		if err != nil {
			return oms, err
		}
		for _, o := range objects.Objects {
			if strings.HasSuffix(o.Key, "/") {
				continue
			}
			oms = append(oms, oss.NewObjectMeta(t.bucket, o.Key))
		}
		if !objects.IsTruncated {
			return oms, nil
		} else {
			marker = objects.NextMarker
		}
	}
}

func (t *bucket) GetObjectSize(path string) (oss.Size, error) {
	bkt, err := t.client.Bucket(t.bucket)
	if err != nil {
		return nil, err
	}
	meta, err := bkt.GetObjectMeta(path)
	if err != nil {
		var serverError aliyun.ServiceError
		if errors.As(err, &serverError) && serverError.Code == "NoSuchKey" {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	size, err := strconv.ParseInt(meta.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}
	return oss.NewSize(size), nil
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
	return "default"
}
