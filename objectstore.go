package osi

type ObjectStore interface {
	Name() string
	Bucket(name string) Bucket
	ACLEnum() ACLEnum
}
