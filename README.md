# osi

A universal object store interface that has implemented s3, oss, minio, cos, obs, and local storage media
[中文版](https://github.com/burybell/osi/blob/master/README_CN.md) 
# Current supported

- [x] s3
- [x] oss
- [x] minio
- [x] cos
- [x] obs
- [x] local (local file system)

# Install

```shell
go get github.com/burybell/osi
```

# Usage
## Initialize Instance
### First Method
use sugar package
```go
store := sugar.MustNewObjectStore(sugar.UseS3(s3.Config{
    Region: "example",
    KeyID:  "example",
    Secret: "example",
}))
```

### Second Method
use implementation package
```go
store,err := minio.NewObjectStore(minio.Config{
    Region: "example",
    KeyID:  "example",
    Secret: "example",
    Endpoint: "localhost:9000",
    UseSSL: false
})
```

## object operation
```go
package main

import (
	"github.com/burybell/osi/s3"
	"github.com/burybell/osi/sugar"
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

## use local file system for testing or other
```go
package main

import (
	"github.com/burybell/osi/local"
	"github.com/burybell/osi/sugar"
	"log"
	"net/http"
	"time"
)

func main() {

	store := sugar.MustNewObjectStore(sugar.UseLocal(local.Config{
		BasePath:   "/tmp/osi",
		HttpAddr:   "http://localhost:8080",
		HttpSecret: "example",
	}))

	//  handle sign url, you can get a signed url for upload or download
	http.HandleFunc("/sign_url", func(w http.ResponseWriter, r *http.Request) {
		url, err := store.Bucket("example").SignURL(r.URL.Query().Get("path"), r.Method, time.Second*1000)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(url))
	})
	log.Fatalln(http.ListenAndServe(":8080", nil))
}
```