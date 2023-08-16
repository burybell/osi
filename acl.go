package oss

type ACL = string

type ACLEnum interface {
	Private() ACL
	PublicRead() ACL
	PublicReadWrite() ACL
	Default() ACL
}
