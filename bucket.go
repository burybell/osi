package oss

import (
	"io"
	"time"
)

type Size interface {
	Size() int64
}

type Bucket interface {
	GetObject(path string) (Object, error)
	PutObject(path string, reader io.Reader) error
	PutObjectWithACL(path string, reader io.Reader, acl ACL) error
	HeadObject(path string) (bool, error)
	DeleteObject(path string) error
	ListObject(prefix string) ([]ObjectMeta, error)
	GetObjectSize(path string) (Size, error)
	SignURL(path string, method string, expiredInDur time.Duration) (string, error)
}

type size int64

func NewSize(sz int64) Size {
	return size(sz)
}

func (t size) Size() int64 {
	return int64(t)
}
