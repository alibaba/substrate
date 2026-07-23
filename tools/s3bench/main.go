// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		bucket      = flag.String("bucket", "", "bucket name")
		prefix      = flag.String("prefix", "", "object prefix")
		totalMiB    = flag.Int("total-mib", 1024, "total payload MiB")
		chunkMiB    = flag.Int("chunk-mib", 64, "chunk size MiB")
		concurrency = flag.Int("concurrency", 16, "concurrent put/get workers")
		pathStyle   = flag.Bool("path-style", true, "use path-style S3 URLs")
		insecureTLS = flag.Bool("insecure-skip-verify", false, "skip TLS certificate verification")
	)
	flag.Parse()
	if *bucket == "" || *prefix == "" {
		log.Fatal("--bucket and --prefix are required")
	}
	if *totalMiB <= 0 || *chunkMiB <= 0 || *concurrency <= 0 {
		log.Fatal("total, chunk, and concurrency must be positive")
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if *insecureTLS {
		cfg.HTTPClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = *pathStyle
	})

	totalBytes := int64(*totalMiB) << 20
	chunkBytes := int64(*chunkMiB) << 20
	chunks := int((totalBytes + chunkBytes - 1) / chunkBytes)

	buf := make([]byte, chunkBytes)
	if _, err := rand.Read(buf); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("config bucket=%s prefix=%s total_mib=%d chunk_mib=%d chunks=%d concurrency=%d\n", *bucket, *prefix, *totalMiB, *chunkMiB, chunks, *concurrency)
	putBytes, putDur, err := putChunks(ctx, client, *bucket, *prefix, buf, totalBytes, chunkBytes, chunks, *concurrency)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("put bytes=%d duration=%s mibps=%.2f\n", putBytes, putDur, mibps(putBytes, putDur))

	getBytes, getDur, err := getChunks(ctx, client, *bucket, *prefix, chunkBytes, chunks, *concurrency)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get bytes=%d duration=%s mibps=%.2f\n", getBytes, getDur, mibps(getBytes, getDur))
}

func putChunks(ctx context.Context, client *s3.Client, bucket, prefix string, buf []byte, totalBytes, chunkBytes int64, chunks, concurrency int) (int64, time.Duration, error) {
	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	var written atomic.Int64
	for i := 0; i < chunks; i++ {
		i := i
		off := int64(i) * chunkBytes
		size := min(chunkBytes, totalBytes-off)
		g.Go(func() error {
			body := bytes.NewReader(buf[:size])
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(path.Join(prefix, fmt.Sprintf("chunk-%08d", i))),
				Body:   body,
			})
			if err != nil {
				return err
			}
			written.Add(size)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return written.Load(), time.Since(start), err
	}
	return written.Load(), time.Since(start), nil
}

func getChunks(ctx context.Context, client *s3.Client, bucket, prefix string, chunkBytes int64, chunks, concurrency int) (int64, time.Duration, error) {
	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	var read atomic.Int64
	for i := 0; i < chunks; i++ {
		i := i
		g.Go(func() error {
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(path.Join(prefix, fmt.Sprintf("chunk-%08d", i))),
			})
			if err != nil {
				return err
			}
			defer out.Body.Close()
			n, err := io.Copy(io.Discard, out.Body)
			if err != nil {
				return err
			}
			read.Add(n)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return read.Load(), time.Since(start), err
	}
	return read.Load(), time.Since(start), nil
}

func mibps(bytes int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(bytes) / 1024 / 1024 / d.Seconds()
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}
