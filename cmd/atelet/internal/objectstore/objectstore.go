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

package objectstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("objectstore")

// ObjectStorage is a cloud-agnostic interface for reading and writing objects.
type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

// Fetch downloads the object at the given URL (s3:// or gs://) and returns
// its contents as a byte slice.
func Fetch(ctx context.Context, client ObjectStorage, storageURL string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "fetch")
	defer span.End()

	bucket, object, err := ParseURL(storageURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("while reading all content: %w", err)
	}

	return content, nil
}

// PutFileWithZstd compresses the file at localFilePath with zstd and uploads
// it to the given object storage URL.
func PutFileWithZstd(ctx context.Context, client ObjectStorage, storageURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "putFileWithZstd")
	defer span.End()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := putWithZstd(ctx, client, storageURL, localFile); err != nil {
		return fmt.Errorf("in putWithZstd: %w", err)
	}

	return nil
}

func putWithZstd(ctx context.Context, client ObjectStorage, storageURL string, content io.Reader) (err error) {
	bucket, object, err := ParseURL(storageURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	// Create a temporary file to store compressed data
	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	zwc, err := zstd.NewWriter(tmpFile)
	if err != nil {
		return fmt.Errorf("while creating zstd writer: %w", err)
	}

	_, err = io.Copy(zwc, content)
	if err != nil {
		zwc.Close()
		return fmt.Errorf("while compressing data to temp file: %w", err)
	}
	if err := zwc.Close(); err != nil {
		return fmt.Errorf("while closing zstd writer: %w", err)
	}

	// Seek back to the beginning of the temp file
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return fmt.Errorf("while seeking temp file: %w", err)
	}

	// Upload the seekable temp file
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return fmt.Errorf("while putting object: %w", err)
	}
	return nil
}

// FetchFileWithZstd downloads an object from the given URL, decompresses it
// with zstd, and writes it to localFilePath.
func FetchFileWithZstd(ctx context.Context, client ObjectStorage, storageURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "fetchFileWithZstd")
	defer span.End()

	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}

	if err := fetchWithZstd(ctx, client, storageURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from object store: %w", storageURL, err)
	}

	return nil
}

func fetchWithZstd(ctx context.Context, client ObjectStorage, storageURL string, out io.Writer) (err error) {
	bucket, object, err := ParseURL(storageURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err != nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	zrc, err := zstd.NewReader(rc, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()

	_, err = io.Copy(out, zrc)
	if err != nil {
		return fmt.Errorf("in io.Copy: %w", err)
	}

	return nil
}

// ParseURL parses an object storage URL (s3:// or gs://) and returns the
// bucket and object path. Both schemes are supported for backward compatibility.
func ParseURL(storageURL string) (string, string, error) {
	parsed, err := url.Parse(storageURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", storageURL, err)
	}

	switch parsed.Scheme {
	case "s3", "gs":
		// both supported
	default:
		return "", "", fmt.Errorf("unsupported URL scheme %q in %q (supported: s3://, gs://)", parsed.Scheme, storageURL)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
