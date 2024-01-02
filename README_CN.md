# osi

一个通用的对象存储接口，已实现了S3、oss（阿里云）, minio（自建）, cos（腾讯云）, obs（华为云）, and local storage media（本地存储）

# 当前支持

- [x] s3
- [x] oss
- [x] minio
- [x] cos
- [x] obs
- [x] local (local file system)

# 安装

```shell
go get github.com/burybell/osi
```

# 使用
## 初始化实例
### 第一种方法
使用 sugar 包
```go
store := sugar.MustNewObjectStore(sugar.UseS3(s3.Config{
    Region: "example",
    KeyID:  "example",
    Secret: "example",
}))
```

### 第二种方法
使用实现类的包
```go
store,err := minio.NewObjectStore(minio.Config{
    Region: "example",
    KeyID:  "example",
    Secret: "example",
    Endpoint: "localhost:9000",
    UseSSL: false
})
```

## 对象操作
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

## 使用本地文件系统用来测试或者其他
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