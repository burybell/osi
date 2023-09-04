package huawei

import (
	"github.com/burybell/oss"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

const (
	Name = "huawei"
)

type Config struct {
	Region   string `yaml:"region" mapstructure:"region" json:"region"`
	KeyID    string `yaml:"key_id" mapstructure:"key_id" json:"key_id"`
	Secret   string `yaml:"secret" mapstructure:"secret" json:"secret"`
	Endpoint string `yaml:"endpoint" mapstructure:"endpoint" json:"endpoint"`
}

type objectstore struct {
	config Config
	client *obs.ObsClient
}

func (o objectstore) Name() string {
	return Name
}

func (o objectstore) Bucket(name string) oss.Bucket {
	//TODO implement me
	panic("implement me")
}

func (o objectstore) ACLEnum() oss.ACLEnum {
	//TODO implement me
	panic("implement me")
}
