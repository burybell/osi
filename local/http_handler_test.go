package local_test

import (
	"fmt"
	"github.com/burybell/oss/local"
	"net/http"
	"testing"
)

func TestSign(t *testing.T) {
	signature := local.Sign(http.MethodPut, "path/to/file", 100, "example")
	fmt.Println(signature)
}
