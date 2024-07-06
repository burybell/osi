package local

import (
	"context"
	"errors"
	"fmt"
	"github.com/burybell/osi"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	Name = "local"
)

type Config struct {
	BasePath   string `yaml:"base_path" mapstructure:"base_path" json:"base_path"`
	HttpAddr   string `yaml:"http_addr" mapstructure:"http_addr" json:"http_addr"`
	HttpSecret string `yaml:"http_secret" mapstructure:"http_secret" json:"http_secret"`
}

type ObjectStore struct {
	config Config
}

func NewObjectStore(config Config) (oss osi.ObjectStore, err error) {
	stat, err := os.Stat(config.BasePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(config.BasePath, os.ModePerm)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		if !stat.IsDir() {
			return nil, errors.New("base path is not a directory")
		}
	}
	defer func() {
		if config.HttpAddr != "" && err == nil {
			HandleHttp(oss.(*ObjectStore), config.HttpSecret)
		}
	}()
	return &ObjectStore{config: config}, nil
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
	bucketPath := fmt.Sprintf("%s/%s", t.config.BasePath, name)
	var bucketErr error
	stat, err := os.Stat(bucketPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(bucketPath, os.ModePerm)
			if err != nil {
				bucketErr = err
			}
		} else {
			bucketErr = err
		}
	} else {
		if !stat.IsDir() {
			bucketErr = errors.New("bucket path is not a directory")
		}
	}

	return &bucket{
		bucket:    name,
		config:    t.config,
		bucketErr: bucketErr,
	}
}

func (t *ObjectStore) ACLEnum() osi.ACLEnum {
	return aclEnum{}
}

type bucket struct {
	config    Config
	bucket    string
	bucketErr error
}

func (t *bucket) fullPath(path string) string {
	return fmt.Sprintf("%s/%s/%s", t.config.BasePath, t.bucket, path)
}

func (t *bucket) GetObject(ctx context.Context, path string) (osi.Object, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}
	file, err := os.Open(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return osi.NewObject(t.bucket, path, strconv.FormatInt(int64(stat.Mode()), 10), file), nil
}

func (t *bucket) PutObject(ctx context.Context, path string, reader io.Reader) error {
	if t.bucketErr != nil {
		return t.bucketErr
	}
	return t.PutObjectWithACL(ctx, path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(ctx context.Context, path string, reader io.Reader, acl osi.ACL) error {
	if t.bucketErr != nil {
		return t.bucketErr
	}

	err := os.MkdirAll(filepath.Dir(t.fullPath(path)), os.ModePerm)
	if err != nil {
		return err
	}

	fileMode := os.FileMode(0644)
	if acl == "0666" {
		fileMode = os.FileMode(0666)
	}

	if acl == "0600" {
		fileMode = os.FileMode(0600)
	}

	file, err := os.OpenFile(t.fullPath(path), os.O_CREATE|os.O_WRONLY, fileMode)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	return err
}

func (t *bucket) HeadObject(ctx context.Context, path string) (bool, error) {
	if t.bucketErr != nil {
		return false, t.bucketErr
	}

	_, err := os.Stat(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return false, osi.ObjectNotFound
		}
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(ctx context.Context, path string) error {
	if t.bucketErr != nil {
		return t.bucketErr
	}
	return os.Remove(t.fullPath(path))
}

func (t *bucket) ListObjects(ctx context.Context, prefix string) ([]osi.ObjectMeta, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}
	var oms = make([]osi.ObjectMeta, 0)
	err := filepath.Walk(t.fullPath(prefix), func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			oms = append(oms, osi.NewObjectMeta(t.bucket, strings.TrimPrefix(path, t.config.BasePath+"/"+t.bucket+"/")))
		}
		return nil
	})
	return oms, err
}

func (t *bucket) GetObjectSize(ctx context.Context, path string) (osi.Size, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}

	stat, err := os.Stat(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, osi.ObjectNotFound
		}
		return nil, err
	}
	return osi.NewSize(stat.Size()), nil
}

func (t *bucket) SignURL(ctx context.Context, path string, method string, expiredInDur time.Duration) (string, error) {
	expires := time.Now().Add(expiredInDur).Unix()
	signature := Sign(method, fmt.Sprintf("%s/%s", t.bucket, path), int(expires), t.config.HttpSecret)
	return fmt.Sprintf("%s/%s/%s?expires=%d&signature=%s", t.config.HttpAddr, t.bucket, path, expires, signature), nil
}

func (t *bucket) DeleteObjects(ctx context.Context, paths []string) error {
	for i := range paths {
		err := os.Remove(t.fullPath(paths[i]))
		if err != nil {
			return err
		}
	}
	return nil
}

type aclEnum struct {
}

func (t aclEnum) Private() osi.ACL {
	return "0600"
}

func (t aclEnum) PublicRead() osi.ACL {
	return "0644"
}

func (t aclEnum) PublicReadWrite() osi.ACL {
	return "0666"
}

func (t aclEnum) Default() osi.ACL {
	return "0644"
}
