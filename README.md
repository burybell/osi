# oss
A universal OSS interface that has implemented s3, aliyun, mini, tent (cos), and local storage media

# current support
- [x] s3
- [x] aliyun
- [x] mini
- [x] tencent (cos)
- [x] local (local file system)
- [ ] qiniu
- [ ] huawei

# example

```go
package main

import (
	"github.com/burybell/oss/s3"
	"github.com/burybell/oss/sugar"
)

func main() {
	store := sugar.MustNewObjectStore(sugar.UseS3(s3.Config{
		Region: "example",
		KeyID:  "example",
		Secret: "example",
	}))
	bucket := store.Bucket("example")
	
	//put object
	err := bucket.PutObject("path/to/file", io.NopCloser(strings.NewReader("some data")))
	if err != nil {
		panic(err)
	}

	// put object with acl
	err = bucket.PutObjectWithACL("path/to/file", io.NopCloser(strings.NewReader("some data")), store.ACLEnum().PublicRead())
	if err != nil {
		panic(err)
	}
	
	// get object
	obj, err := bucket.GetObject("path/to/file")
	if err != nil {
		panic(err)
	}

	// get object size
	sz, err := bucket.GetObjectSize("path/to/file")
	if err != nil {
		panic(err)
	}
	fmt.Println(sz.Size())

	// check object exist
	exist, err := bucket.HeadObject("path/to/file")
	if err != nil {
		panic(err)
	}
	fmt.Println(exist)
	
	f, err := os.Open("path/to/file")
	if err != nil {
		panic(err)
	}
	_, _ = io.Copy(f, obj)
	_ = f.Close()
    
	// list object
	objects, err := bucket.ListObject("/path")
	if err != nil {
		panic(err)
	}

	for i := range objects {
		// delete object
		_ = bucket.DeleteObject(objects[i].ObjectPath())
	}

	url, err := bucket.SignURL("path/to/file", http.MethodGet, time.Second*100)
	if err != nil {
		panic(err)
	}
	fmt.Println(url)
}

```