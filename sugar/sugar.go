package sugar

import (
	"errors"
	"github.com/burybell/oss"
	"github.com/burybell/oss/aliyun"
	"github.com/burybell/oss/local"
	"github.com/burybell/oss/minio"
	"github.com/burybell/oss/s3"
	"github.com/burybell/oss/tencent"
)

type Options struct {
	AliYun  aliyun.Config
	S3      s3.Config
	Tencent tencent.Config
	Local   local.Config
	Minio   minio.Config
	UseName string
}

type Option func(opts *Options)

func UseAliYun(config aliyun.Config) Option {
	return func(opts *Options) {
		opts.AliYun = config
		opts.UseName = aliyun.Name
	}
}

func UseS3(config s3.Config) Option {
	return func(opts *Options) {
		opts.S3 = config
		opts.UseName = s3.Name
	}
}

func UseLocal(config local.Config) Option {
	return func(opts *Options) {
		opts.Local = config
		opts.UseName = local.Name
	}
}

func UseMinio(config minio.Config) Option {
	return func(opts *Options) {
		opts.Minio = config
		opts.UseName = minio.Name
	}
}

func UseTencent(config tencent.Config) Option {
	return func(opts *Options) {
		opts.Tencent = config
		opts.UseName = tencent.Name
	}
}

func NewObjectStore(opt ...Option) (oss.ObjectStore, error) {
	opts := &Options{}
	for _, opt := range opt {
		opt(opts)
	}

	if opts.UseName == "" {
		opts.UseName = local.Name
		opts.Local = local.Config{BasePath: "/tmp"}
	}

	switch opts.UseName {
	case aliyun.Name:
		return aliyun.NewObjectStore(opts.AliYun)
	case s3.Name:
		return s3.NewObjectStore(opts.S3)
	case tencent.Name:
		return tencent.NewObjectStore(opts.Tencent)
	case local.Name:
		return local.NewObjectStore(opts.Local)
	case minio.Name:
		return minio.NewObjectStore(opts.Minio)
	default:
		return nil, errors.New("no support objectstore")
	}
}

func MustNewObjectStore(opt ...Option) oss.ObjectStore {
	store, err := NewObjectStore(opt...)
	if err != nil {
		panic(err)
	}
	return store
}
