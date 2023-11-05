package obs

import (
	"fmt"
	"github.com/burybell/osi"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	Name = "obs"
)

type Config struct {
	Region   string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID    string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret   string `yaml:"secret" mapstructure:"secret" json:"secret"`
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint" json:"endpoint"`
}

type ObjectStore struct {
	config Config
	client *obs.ObsClient
}

func NewObjectStore(config Config) (osi.ObjectStore, error) {
	if config.Endpoint == "" {
		config.Endpoint = fmt.Sprintf("https://obs.%s.myhuaweicloud.com", config.Region)
	}
	client, err := obs.New(config.KeyID, config.Secret, config.Endpoint)
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
	client *obs.ObsClient
	bucket string
}

func (t *bucket) GetObject(path string) (osi.Object, error) {
	acl, err := t.client.GetObjectAcl(&obs.GetObjectAclInput{Bucket: t.bucket, Key: path})
	if err != nil {
		if err.(obs.ObsError).Code == "NoSuchKey" {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}

	var publicACL = make(map[obs.PermissionType]int)
	for _, grant := range acl.Grants {
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

	resp, err := t.client.GetObject(&obs.GetObjectInput{GetObjectMetadataInput: obs.GetObjectMetadataInput{Bucket: t.bucket, Key: path}})
	if err != nil {
		return nil, err
	}
	return osi.NewObject(t.bucket, path, ACL, resp.Body), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl osi.ACL) error {
	_, err := t.client.PutObject(&obs.PutObjectInput{PutObjectBasicInput: obs.PutObjectBasicInput{ObjectOperationInput: obs.ObjectOperationInput{Bucket: t.bucket, Key: path, ACL: obs.AclType(acl)}}, Body: reader})
	return err
}

func (t *bucket) HeadObject(path string) (bool, error) {
	_, err := t.client.HeadObject(&obs.HeadObjectInput{Bucket: t.bucket, Key: path})
	if err != nil {
		if err.(obs.ObsError).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(path string) error {
	_, err := t.client.DeleteObject(&obs.DeleteObjectInput{Bucket: t.bucket, Key: path})
	return err
}

func (t *bucket) ListObject(prefix string) ([]osi.ObjectMeta, error) {
	var oms = make([]osi.ObjectMeta, 0)
	var marker = ""
	for {
		objects, err := t.client.ListObjects(&obs.ListObjectsInput{
			ListObjsInput: obs.ListObjsInput{Prefix: prefix, MaxKeys: 200},
			Bucket:        t.bucket,
			Marker:        marker,
		})
		if err != nil {
			return oms, err
		}
		for _, key := range objects.Contents {
			if key.Key != "" {
				if strings.HasSuffix(key.Key, "/") {
					continue
				}
				oms = append(oms, osi.NewObjectMeta(t.bucket, key.Key))
			}
		}
		if !objects.IsTruncated {
			return oms, nil
		} else {
			if objects.NextMarker != "" {
				marker = objects.NextMarker
			} else {
				return oms, nil
			}
		}
	}
}

func (t *bucket) GetObjectSize(path string) (osi.Size, error) {
	resp, err := t.client.HeadObject(&obs.HeadObjectInput{Bucket: t.bucket, Key: path})
	if err != nil {
		if err.(obs.ObsError).Code == "NoSuchKey" {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}

	if len(resp.ResponseHeaders["content-length"]) > 0 {
		sz, err := strconv.ParseInt(resp.ResponseHeaders["content-length"][0], 10, 64)
		if err != nil {
			return nil, err
		}
		return osi.NewSize(sz), nil
	}
	return osi.NewSize(0), nil
}

func (t *bucket) SignURL(path string, method string, expiredInDur time.Duration) (string, error) {
	url, err := t.client.CreateSignedUrl(&obs.CreateSignedUrlInput{
		Method:  obs.HttpMethodType(method),
		Bucket:  t.bucket,
		Key:     path,
		Expires: int(expiredInDur.Seconds()),
	})
	if err != nil {
		return "", err
	}
	return url.SignedUrl, nil
}

type aclEnum struct {
}

func (t aclEnum) Private() osi.ACL {
	return osi.ACL(obs.AclPrivate)
}

func (t aclEnum) PublicRead() osi.ACL {
	return osi.ACL(obs.AclPublicRead)
}

func (t aclEnum) PublicReadWrite() osi.ACL {
	return osi.ACL(obs.AclPublicReadWrite)
}

func (t aclEnum) Default() osi.ACL {
	return ""
}
