package s3

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/burybell/osi"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	Name = "s3"
)

type Config struct {
	Region string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID  string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret string `yaml:"secret" mapstructure:"secret" json:"secret"`
}

type ObjectStore struct {
	config Config
	client *s3.S3
}

func NewObjectStore(config Config) (osi.ObjectStore, error) {
	provider, err := session.NewSession(aws.NewConfig().WithRegion(config.Region).WithCredentials(credentials.NewStaticCredentials(config.KeyID, config.Secret, "")))
	if err != nil {
		return nil, err
	}
	return &ObjectStore{config: config, client: s3.New(provider)}, nil
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
	client *s3.S3
	bucket string
}

func (t *bucket) GetObject(ctx context.Context, path string) (osi.Object, error) {
	acl, err := t.client.GetObjectAcl(&s3.GetObjectAclInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NoSuchKey" {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}

	var publicACL = make(map[string]int)
	for _, grant := range acl.Grants {
		publicACL[*grant.Permission] = 1
	}

	var ACL = ""
	if publicACL["FULL_CONTROL"] == 1 || (publicACL["READ"] == 1 && publicACL["WRITE"] == 1) {
		ACL = "public-read-write"
	} else if publicACL["READ"] == 1 {
		ACL = "public-read"
	} else {
		ACL = "private"
	}

	resp, err := t.client.GetObject(&s3.GetObjectInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		return nil, err
	}
	return osi.NewObject(t.bucket, path, ACL, resp.Body), nil
}

func (t *bucket) PutObject(ctx context.Context, path string, reader io.Reader) error {
	return t.PutObjectWithACL(ctx, path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(ctx context.Context, path string, reader io.Reader, acl osi.ACL) error {
	temp, err := os.CreateTemp("", "temp")
	if err != nil {
		return err
	}
	_, _ = io.Copy(temp, reader)
	_ = temp.Close()

	f, err := os.Open(temp.Name())
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(temp.Name())
	}()
	_, err = t.client.PutObject(&s3.PutObjectInput{
		Bucket:      &t.bucket,
		Key:         &path,
		Body:        f,
		ContentType: aws.String(mime.TypeByExtension(filepath.Ext(path))),
		ACL:         aws.String(acl),
	})
	return err
}

func (t *bucket) HeadObject(ctx context.Context, path string) (bool, error) {
	_, err := t.client.HeadObject(&s3.HeadObjectInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(ctx context.Context, path string) error {
	_, err := t.client.DeleteObject(&s3.DeleteObjectInput{Bucket: &t.bucket, Key: &path})
	return err
}

func (t *bucket) ListObjects(ctx context.Context, prefix string) ([]osi.ObjectMeta, error) {

	var oms = make([]osi.ObjectMeta, 0)
	var marker = ""
	for {
		objects, err := t.client.ListObjects(&s3.ListObjectsInput{
			Bucket:  aws.String(t.bucket),
			Prefix:  aws.String(prefix),
			Marker:  aws.String(marker),
			MaxKeys: aws.Int64(200),
		})
		if err != nil {
			return oms, err
		}
		for _, key := range objects.Contents {
			if key.Key != nil {
				if strings.HasSuffix(*key.Key, "/") {
					continue
				}
				oms = append(oms, osi.NewObjectMeta(t.bucket, *key.Key))
			}
		}
		if objects.IsTruncated != nil && !*objects.IsTruncated {
			return oms, nil
		} else {
			if objects.NextMarker != nil {
				marker = *objects.NextMarker
			} else {
				return oms, nil
			}
		}
	}
}

func (t *bucket) GetObjectSize(ctx context.Context, path string) (osi.Size, error) {
	resp, err := t.client.HeadObject(&s3.HeadObjectInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NotFound" {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}
	return osi.NewSize(*resp.ContentLength), nil
}

func (t *bucket) SignURL(ctx context.Context, path string, method string, expiredInDur time.Duration) (string, error) {

	var req *request.Request
	switch method {
	case http.MethodGet:
		req, _ = t.client.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    aws.String(path),
		})
		return req.Presign(expiredInDur)
	case http.MethodPut:
		req, _ = t.client.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    aws.String(path),
		})
		return req.Presign(expiredInDur)
	case http.MethodDelete:
		req, _ = t.client.DeleteObjectRequest(&s3.DeleteObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    aws.String(path),
		})
		return req.Presign(expiredInDur)
	case http.MethodHead:
		req, _ = t.client.HeadObjectRequest(&s3.HeadObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    aws.String(path),
		})
		return req.Presign(expiredInDur)
	default:
		return "", errors.New("not support")
	}
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
	input := &s3.DeleteObjectsInput{Bucket: &t.bucket, Delete: &s3.Delete{}}
	for i := range paths {
		input.Delete.Objects = append(input.Delete.Objects, &s3.ObjectIdentifier{
			Key: aws.String(paths[i]),
		})
	}
	_, err := t.client.DeleteObjects(input)
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
	return ""
}
