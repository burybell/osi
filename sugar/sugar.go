package sugar

import (
	"errors"
	"github.com/burybell/osi"
	"github.com/burybell/osi/cos"
	"github.com/burybell/osi/local"
	"github.com/burybell/osi/minio"
	"github.com/burybell/osi/obs"
	"github.com/burybell/osi/oss"
	"github.com/burybell/osi/s3"
)

type Options struct {
	OSS     oss.Config
	S3      s3.Config
	COS     cos.Config
	Local   local.Config
	Minio   minio.Config
	OBS     obs.Config
	UseName string
}

type Option func(opts *Options)

func UseOSS(config oss.Config) Option {
	return func(opts *Options) {
		opts.OSS = config
		opts.UseName = oss.Name
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

func UseCOS(config cos.Config) Option {
	return func(opts *Options) {
		opts.COS = config
		opts.UseName = cos.Name
	}
}

func UseOBS(config obs.Config) Option {
	return func(opts *Options) {
		opts.OBS = config
		opts.UseName = obs.Name
	}
}

func NewObjectStore(opt ...Option) (osi.ObjectStore, error) {
	opts := &Options{}
	for _, opt := range opt {
		opt(opts)
	}

	if opts.UseName == "" {
		opts.UseName = local.Name
		opts.Local = local.Config{BasePath: "/tmp"}
	}

	switch opts.UseName {
	case oss.Name:
		return oss.NewObjectStore(opts.OSS)
	case s3.Name:
		return s3.NewObjectStore(opts.S3)
	case cos.Name:
		return cos.NewObjectStore(opts.COS)
	case local.Name:
		return local.NewObjectStore(opts.Local)
	case minio.Name:
		return minio.NewObjectStore(opts.Minio)
	case obs.Name:
		return obs.NewObjectStore(opts.OBS)
	default:
		return nil, errors.New("no support object store")
	}
}

func MustNewObjectStore(opt ...Option) osi.ObjectStore {
	store, err := NewObjectStore(opt...)
	if err != nil {
		panic(err)
	}
	return store
}
