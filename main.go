package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

const BigqueryTimestampLayout = "2006-01-02 15:04:05"

type Param struct {
	Bucket string
}

func main() {
	param, err := getFlag()
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	ctx := context.Background()

	wd, err := os.Getwd()
	if err != nil {
		fmt.Printf("failed get working dir. err=%+v\n", err)
	}

	fn, err := run(ctx, param, wd)
	if err != nil {
		fmt.Printf("failed: +%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("output %s", fn)
}

func run(ctx context.Context, param *Param, output string) (fileName string, rerr error) {
	fn, f, err := NewCSVFile(output, param.Bucket)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			if rerr == nil {
				rerr = errors.WithStack(err)
				return
			}
			fmt.Printf("failed file.Close() err=%+v\n", err)
		}
	}()
	if err := ls(ctx, param.Bucket, f); err != nil {
		return "", errors.Wrap(err, "failed ls to gcs with output file.")
	}

	return fn, nil
}

func ls(ctx context.Context, bucket string, out io.Writer) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	w := csv.NewWriter(out)
	if err := w.Write([]string{"Name", "Size", "Generation", "StorageClass", "Created", "Deleted"}); err != nil {
		return err
	}
	query := &storage.Query{
		Versions: true,
	}

	var count int
	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		count++
		if count > 1000 {
			w.Flush()
			count = 0
		}
		row := []string{attrs.Name, fmt.Sprintf("%d", attrs.Size), fmt.Sprintf("%d", attrs.Generation), attrs.StorageClass, attrs.Created.Format(BigqueryTimestampLayout)}
		if !attrs.Deleted.IsZero() {
			row = append(row, attrs.Deleted.Format(BigqueryTimestampLayout))
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func getFlag() (*Param, error) {
	var (
		bucket = flag.String("bucket", "", "cloud storage bucket")
	)
	flag.Parse()

	if len(*bucket) < 1 {
		return nil, fmt.Errorf("bucket is required\n")
	}

	return &Param{
		Bucket: *bucket,
	}, nil
}
