package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"oss"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	Name = "local"
)

type Config struct {
	BasePath string `yaml:"base_path" mapstructure:"base_path" json:"base_path"`
}

type objectstore struct {
	config Config
}

func NewObjectStore(config Config) (oss.ObjectStore, error) {
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
	return &objectstore{config: config}, nil
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

func (t *objectstore) ACLEnum() oss.ACLEnum {
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

func (t *bucket) GetObject(path string) (oss.Object, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}
	file, err := os.Open(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return oss.NewObject(t.bucket, path, strconv.FormatInt(int64(stat.Mode()), 10), file), nil
}

func (t *bucket) PutObject(path string, reader io.Reader) error {
	if t.bucketErr != nil {
		return t.bucketErr
	}
	return t.PutObjectWithACL(path, reader, aclEnum{}.Default())
}

func (t *bucket) PutObjectWithACL(path string, reader io.Reader, acl oss.ACL) error {
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

func (t *bucket) HeadObject(path string) (bool, error) {
	if t.bucketErr != nil {
		return false, t.bucketErr
	}

	_, err := os.Stat(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return false, oss.ObjectNotFound
		}
		return false, err
	}
	return true, nil
}

func (t *bucket) DeleteObject(path string) error {
	if t.bucketErr != nil {
		return t.bucketErr
	}
	return os.Remove(t.fullPath(path))
}

func (t *bucket) ListObject(prefix string) ([]oss.ObjectMeta, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}
	var oms = make([]oss.ObjectMeta, 0)
	err := filepath.Walk(t.fullPath(prefix), func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			oms = append(oms, oss.NewObjectMeta(t.bucket, strings.TrimPrefix(path, t.config.BasePath+"/"+t.bucket+"/")))
		}
		return nil
	})
	return oms, err
}

func (t *bucket) GetObjectSize(path string) (oss.Size, error) {
	if t.bucketErr != nil {
		return nil, t.bucketErr
	}

	stat, err := os.Stat(t.fullPath(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, oss.ObjectNotFound
		}
		return nil, err
	}
	return oss.NewSize(stat.Size()), nil
}

type aclEnum struct {
}

func (t aclEnum) Private() oss.ACL {
	return "0600"
}

func (t aclEnum) PublicRead() oss.ACL {
	return "0644"
}

func (t aclEnum) PublicReadWrite() oss.ACL {
	return "0666"
}

func (t aclEnum) Default() oss.ACL {
	return "0644"
}
