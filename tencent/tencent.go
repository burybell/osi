package tencent

import (
	"context"
	"fmt"
	"github.com/burybell/oss"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	Name = "tencent"
)

type Config struct {
	Region string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID  string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret string `yaml:"secret" mapstructure:"secret" json:"secret"`
}

type objectstore struct {
	config Config
	client *cos.Client
}

func NewObjectStore(config Config) (oss.ObjectStore, error) {
	su, err := url.Parse(fmt.Sprintf("https://cos.%s.myqcloud.com", config.Region))
	if err != nil {
		return nil, err
	}
	b := &cos.BaseURL{ServiceURL: su}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  config.KeyID,
			SecretKey: config.Secret,
		},
	})
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
	bucketURL, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", name, t.config.Region))
	return &bucket{
		config: t.config,
		client: cos.NewClient(&cos.BaseURL{
			ServiceURL: t.client.BaseURL.ServiceURL,
			BucketURL:  bucketURL,
		}, &http.Client{
			Transport: &cos.AuthorizationTransport{
				SecretID:  t.config.KeyID,
				SecretKey: t.config.Secret,
			},
		}),
		bucket: name,
	}
}

func (t *objectstore) ACLEnum() oss.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config Config
	client *cos.Client
	bucket string
}

func (t *bucket) GetObject(path string) (oss.Object, error) {
	acl, resp, err := t.client.Object.GetACL(context.TODO(), path)
	if err != nil {
		if cos.IsNotFoundError(err) {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	_ = resp.Body.Close()

	publicACL := make(map[string]int)
	for _, access := range acl.AccessControlList {
		publicACL[access.Permission] = 1
	}

	resACL := ""
	if publicACL["FULL_CONTROL"] == 1 || (publicACL["READ"] == 1 && publicACL["WRITE"] == 1) {
		resACL = "public-read-write"
	} else if publicACL["READ"] == 1 {
		resACL = "public-read"
	} else {
		resACL = "private"
	}

	resp, err = t.client.Object.Get(context.TODO(), path, nil)
	if err != nil {
		return nil, err
	}
	return oss.NewObject(t.bucket, path, resACL, resp.Body), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl oss.ACL) error {
	_, err := t.client.Object.Put(context.TODO(), path, reader, &cos.ObjectPutOptions{
		ACLHeaderOptions: &cos.ACLHeaderOptions{
			XCosACL: acl,
		},
	})
	return err
}

func (t *bucket) HeadObject(path string) (bool, error) {
	return t.client.Object.IsExist(context.TODO(), path)
}

func (t *bucket) DeleteObject(path string) error {
	_, err := t.client.Object.Delete(context.TODO(), path, nil)
	return err
}

func (t *bucket) ListObject(prefix string) ([]oss.ObjectMeta, error) {
	var oms = make([]oss.ObjectMeta, 0)
	var marker = ""
	for {
		resp, _, err := t.client.Bucket.Get(context.TODO(), &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 200,
		})
		if err != nil {
			return oms, err
		}
		for _, object := range resp.Contents {
			if strings.HasSuffix(object.Key, "/") {
				continue
			}
			oms = append(oms, oss.NewObjectMeta(t.bucket, object.Key))
		}
		if !resp.IsTruncated {
			return oms, nil
		} else {
			marker = resp.NextMarker
		}
	}
}

func (t *bucket) GetObjectSize(path string) (oss.Size, error) {
	resp, err := t.client.Object.Head(context.TODO(), path, nil)
	if err != nil {
		if cos.IsNotFoundError(err) {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	return oss.NewSize(resp.ContentLength), nil
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
