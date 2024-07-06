package cos

import (
	"context"
	"fmt"
	"github.com/burybell/osi"
	"github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	Name = "cos"
)

type Config struct {
	Region string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID  string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret string `yaml:"secret" mapstructure:"secret" json:"secret"`
}

type ObjectStore struct {
	config Config
	client *cos.Client
}

func NewObjectStore(config Config) (osi.ObjectStore, error) {
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

func (t *ObjectStore) ACLEnum() osi.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config Config
	client *cos.Client
	bucket string
}

func (t *bucket) GetObject(ctx context.Context, path string) (osi.Object, error) {
	acl, resp, err := t.client.Object.GetACL(ctx, path)
	if err != nil {
		if cos.IsNotFoundError(err) {
			return nil, osi.ObjectNotFound
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
	return osi.NewObject(t.bucket, path, resACL, resp.Body), nil
}

func (t *bucket) PutObject(ctx context.Context, path string, reader io.Reader) error {
	return t.PutObjectWithACL(ctx, path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(ctx context.Context, path string, reader io.Reader, acl osi.ACL) error {
	_, err := t.client.Object.Put(ctx, path, reader, &cos.ObjectPutOptions{
		ACLHeaderOptions: &cos.ACLHeaderOptions{
			XCosACL: acl,
		},
	})
	return err
}

func (t *bucket) HeadObject(ctx context.Context, path string) (bool, error) {
	return t.client.Object.IsExist(ctx, path)
}

func (t *bucket) DeleteObject(ctx context.Context, path string) error {
	_, err := t.client.Object.Delete(ctx, path, nil)
	return err
}

func (t *bucket) ListObjects(ctx context.Context, prefix string) ([]osi.ObjectMeta, error) {
	var oms = make([]osi.ObjectMeta, 0)
	var marker = ""
	for {
		resp, _, err := t.client.Bucket.Get(ctx, &cos.BucketGetOptions{
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
			oms = append(oms, osi.NewObjectMeta(t.bucket, object.Key))
		}
		if !resp.IsTruncated {
			return oms, nil
		} else {
			marker = resp.NextMarker
		}
	}
}

func (t *bucket) GetObjectSize(ctx context.Context, path string) (osi.Size, error) {
	resp, err := t.client.Object.Head(ctx, path, nil)
	if err != nil {
		if cos.IsNotFoundError(err) {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}
	return osi.NewSize(resp.ContentLength), nil
}

func (t *bucket) SignURL(ctx context.Context, path string, method string, expiredInDur time.Duration) (string, error) {
	rawURL, err := t.client.Object.GetPresignedURL(ctx, method, path, t.config.KeyID, t.config.Secret, expiredInDur, nil)
	if err != nil {
		return "", err
	}
	return rawURL.String(), nil
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
	opts := &cos.ObjectDeleteMultiOptions{}
	for i := range paths {
		opts.Objects = append(opts.Objects, cos.Object{
			Key: paths[i],
		})
	}
	_, _, err := t.client.Object.DeleteMulti(ctx, opts)
	return err
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
	return "default"
}
