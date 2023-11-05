package osi

import (
	"io"
	"path/filepath"
)

type ObjectMeta interface {
	Bucket() string
	ObjectPath() string
	Extension() string
}

type Object interface {
	ObjectMeta
	io.ReadCloser
	ObjectACL() ACL
}

type objectMeta struct {
	bucket string
	path   string
}

func NewObjectMeta(bucket string, path string) ObjectMeta {
	return &objectMeta{bucket: bucket, path: path}
}

func (t *objectMeta) Bucket() string {
	return t.bucket
}

func (t *objectMeta) ObjectPath() string {
	return t.path
}

func (t *objectMeta) Extension() string {
	return filepath.Ext(t.path)
}

type object struct {
	ObjectMeta
	acl string
	io.ReadCloser
}

func NewObject(bucket string, path string, acl ACL, reader io.ReadCloser) Object {
	return &object{ObjectMeta: NewObjectMeta(bucket, path), acl: acl, ReadCloser: reader}
}

func (t *object) ObjectACL() ACL {
	return t.acl
}
