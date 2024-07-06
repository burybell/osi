package osi

import (
	"context"
	"io"
	"time"
)

type Size interface {
	Size() int64
}

type Bucket interface {
	BucketObject
	BucketObjects
	ObjectSigner
}

type BucketObject interface {
	GetObject(ctx context.Context, path string) (Object, error)
	PutObject(ctx context.Context, path string, reader io.Reader) error
	PutObjectWithACL(ctx context.Context, path string, reader io.Reader, acl ACL) error
	HeadObject(ctx context.Context, path string) (bool, error)
	DeleteObject(ctx context.Context, path string) error
	GetObjectSize(ctx context.Context, path string) (Size, error)
}

type BucketObjects interface {
	ListObjects(ctx context.Context, prefix string) ([]ObjectMeta, error)
	DeleteObjects(ctx context.Context, paths []string) error
}

type ObjectSigner interface {
	SignURL(ctx context.Context, path string, method string, expiredInDur time.Duration) (string, error)
}

type size int64

func NewSize(sz int64) Size {
	return size(sz)
}

func (t size) Size() int64 {
	return int64(t)
}
