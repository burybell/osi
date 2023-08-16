package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"mime"
	"os"
	"oss"
	"path/filepath"
	"strings"
)

const (
	Name = "s3"
)

type Config struct {
	Region string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID  string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret string `yaml:"secret" mapstructure:"secret" json:"secret"`
}

type objectstore struct {
	config Config
	client *s3.S3
}

func NewObjectStore(config Config) (oss.ObjectStore, error) {
	provider, err := session.NewSession(aws.NewConfig().WithRegion(config.Region).WithCredentials(credentials.NewStaticCredentials(config.KeyID, config.Secret, "")))
	if err != nil {
		return nil, err
	}
	return &objectstore{config: config, client: s3.New(provider)}, nil
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
	client *s3.S3
	bucket string
}

func (t *bucket) GetObject(path string) (oss.Object, error) {
	acl, err := t.client.GetObjectAcl(&s3.GetObjectAclInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NoSuchKey" {
			return nil, oss.ObjectNotFound
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
	return oss.NewObject(t.bucket, path, ACL, resp.Body), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl oss.ACL) error {
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

func (t *bucket) HeadObject(path string) (bool, error) {
	_, err := t.client.HeadObject(&s3.HeadObjectInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(path string) error {
	_, err := t.client.DeleteObject(&s3.DeleteObjectInput{Bucket: &t.bucket, Key: &path})
	return err
}

func (t *bucket) ListObject(prefix string) ([]oss.ObjectMeta, error) {

	var oms = make([]oss.ObjectMeta, 0)
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
				oms = append(oms, oss.NewObjectMeta(t.bucket, *key.Key))
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

func (t *bucket) GetObjectSize(path string) (oss.Size, error) {
	resp, err := t.client.HeadObject(&s3.HeadObjectInput{Bucket: &t.bucket, Key: &path})
	if err != nil {
		if err.(awserr.Error).Code() == "NotFound" {
			return nil, nil
		}
		return nil, err
	}
	return oss.NewSize(*resp.ContentLength), nil
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
