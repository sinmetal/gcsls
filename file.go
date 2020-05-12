package main

import (
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
)

func NewCSVFile(path string, bucket string) (string, *os.File, error) {
	fn := fmt.Sprintf("%s/gcsls-%s-%s.csv", path, bucket, time.Now().Format("20060102150405"))
	f, err := os.Create(fn)
	if err != nil {
		return "", nil, errors.WithStack(err)
	}
	return fn, f, nil
}
