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
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Client struct {
	client *awss3.Client
}

// NewS3Client wraps an AWS S3 client in the ObjectStorage interface.
func NewS3Client(client *awss3.Client) ObjectStorage {
	return &s3Client{client: client}
}

func (s *s3Client) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Client) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	})
	return err
}

// NewClient creates an ObjectStorage client based on environment variables.
//
// Configuration is done through ATE_OBJECTSTORE_* env vars:
//   - ATE_OBJECTSTORE_ENDPOINT       → AWS_ENDPOINT_URL_S3
//   - ATE_OBJECTSTORE_REGION         → AWS_REGION
//   - ATE_OBJECTSTORE_ACCESS_KEY_ID  → AWS_ACCESS_KEY_ID
//   - ATE_OBJECTSTORE_SECRET_ACCESS_KEY → AWS_SECRET_ACCESS_KEY
//   - ATE_OBJECTSTORE_USE_PATH_STYLE → enables path-style addressing when "true"
//
// For backward compatibility, ATE_STORAGE_BACKEND=gcs is still accepted and
// auto-configures the GCS S3-compatible endpoint.
func NewClient(ctx context.Context) (ObjectStorage, error) {
	// Backward-compat: warn if legacy env var is set.
	storageBackend := os.Getenv("ATE_STORAGE_BACKEND")
	if storageBackend != "" {
		slog.WarnContext(ctx, "ATE_STORAGE_BACKEND is deprecated; use ATE_OBJECTSTORE_* env vars instead",
			slog.String("ATE_STORAGE_BACKEND", storageBackend))
	}

	// Map ATE_OBJECTSTORE_* → AWS SDK env vars (set only if not already set).
	envMappings := []struct{ src, dst string }{
		{"ATE_OBJECTSTORE_ENDPOINT", "AWS_ENDPOINT_URL_S3"},
		{"ATE_OBJECTSTORE_REGION", "AWS_REGION"},
		{"ATE_OBJECTSTORE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"},
		{"ATE_OBJECTSTORE_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"},
	}
	for _, m := range envMappings {
		if v := os.Getenv(m.src); v != "" {
			if err := os.Setenv(m.dst, v); err != nil {
				return nil, fmt.Errorf("while setting %s: %w", m.dst, err)
			}
		}
	}

	// Backward-compat: ATE_STORAGE_BACKEND=gcs → use GCS S3-compatible endpoint.
	if storageBackend == "gcs" && os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		slog.InfoContext(ctx, "ATE_STORAGE_BACKEND=gcs detected; auto-configuring GCS S3-compatible endpoint")
		if err := os.Setenv("AWS_ENDPOINT_URL_S3", "https://storage.googleapis.com"); err != nil {
			return nil, fmt.Errorf("while setting AWS_ENDPOINT_URL_S3 for GCS: %w", err)
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("while loading AWS config: %w", err)
	}

	usePathStyle := os.Getenv("ATE_OBJECTSTORE_USE_PATH_STYLE") == "true"
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if usePathStyle {
			o.UsePathStyle = true
		}
	})

	return NewS3Client(client), nil
}
