package local

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/burybell/osi"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

type HttpHandler struct {
	Secret string
	store  *ObjectStore
}

func (t *HttpHandler) GetHandler(w http.ResponseWriter, r *http.Request) {
	bkt, path, err := t.GetBucketAndPath(r)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	object, err := t.store.Bucket(bkt).GetObject(path)
	if err != nil {
		if errors.Is(err, osi.ObjectNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", mime.TypeByExtension(object.Extension()))
	defer object.Close()
	_, _ = io.Copy(w, object)
}

func (t *HttpHandler) PutHandler(w http.ResponseWriter, r *http.Request) {
	bkt, path, err := t.GetBucketAndPath(r)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = t.store.Bucket(bkt).PutObject(path, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (t *HttpHandler) HeadHandler(w http.ResponseWriter, r *http.Request) {
	bkt, path, err := t.GetBucketAndPath(r)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	exist, err := t.store.Bucket(bkt).HeadObject(path)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if exist {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (t *HttpHandler) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	bkt, path, err := t.GetBucketAndPath(r)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = t.store.Bucket(bkt).DeleteObject(path)
	if err != nil {
		if errors.Is(err, osi.ObjectNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (t *HttpHandler) GetBucketAndPath(r *http.Request) (string, string, error) {
	items := strings.Split(r.URL.Path, "/")
	if len(items) <= 1 {
		return "", "", fmt.Errorf("invalid path")
	}
	return items[0], strings.Join(items[1:], "/"), nil
}

func HandleHttp(store *ObjectStore, secret string) {
	handler := HttpHandler{Secret: secret, store: store}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		expires, err := strconv.ParseInt(r.URL.Query().Get("expires"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		signature := Sign(r.Method, strings.TrimPrefix(r.URL.Path, "/"), int(expires), secret)
		if signature != r.URL.Query().Get("signature") {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handler.GetHandler(w, r)
		case http.MethodPut:
			handler.PutHandler(w, r)
		case http.MethodHead:
			handler.HeadHandler(w, r)
		case http.MethodDelete:
			handler.DeleteHandler(w, r)
		}
	})
}

func Sign(method string, path string, expires int, secret string) string {
	var buf strings.Builder
	buf.WriteString(method)
	buf.WriteRune('\n')
	buf.WriteString(path)
	buf.WriteRune('\n')
	buf.WriteString(fmt.Sprintf("%d", expires))
	buf.WriteRune('\n')
	buf.WriteString(secret)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(base64.StdEncoding.EncodeToString([]byte(buf.String())))))
}
