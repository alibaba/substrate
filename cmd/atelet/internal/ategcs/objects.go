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

package ategcs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("ategcs")

func snapshotDecoderConcurrency() int {
	raw := strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_DECODER_CONCURRENCY"))
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 64 {
		return 1
	}
	return n
}

var snapshotFSReadRetryDelays = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
	3200 * time.Millisecond,
	6400 * time.Millisecond,
}

var snapshotFSRenameRetryDelays = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
}

var snapshotFSMkdirAll = os.MkdirAll
var snapshotFSMkdir = os.Mkdir
var snapshotFSStat = os.Stat
var snapshotFSRename = os.Rename

type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

func SnapshotFSConfigured() bool {
	return fsSnapshotRoot() != ""
}

func FetchFromGCS(ctx context.Context, client ObjectStorage, gsURL string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "fetchFromGCS")
	defer span.End()

	if fsSnapshotRoot() != "" {
		content, err := fetchBytesFromSnapshotFS(ctx, gsURL)
		if err == nil {
			return content, nil
		}
		if !snapshotFSFallbackEnabled() {
			return nil, err
		}
		slog.WarnContext(ctx, "Snapshot fs read failed; falling back to object storage", slog.String("url", gsURL), slog.Any("err", err))
	}

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
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

// Open streams the object at gsURL; the caller must Close the returned reader.
// Unlike FetchFromGCS it does not buffer the whole object in memory.
func Open(ctx context.Context, client ObjectStorage, gsURL string) (io.ReadCloser, error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return rc, nil
}

// SendBytesToGCS uploads the given bytes (uncompressed) to gsURL. Intended for
// small objects such as the snapshot manifest.
func SendBytesToGCS(ctx context.Context, client ObjectStorage, gsURL string, content []byte) error {
	ctx, span := tracer.Start(ctx, "sendBytesToGCS")
	defer span.End()

	if fsSnapshotRoot() != "" {
		if err := sendBytesToSnapshotFS(ctx, gsURL, content); err != nil {
			return err
		}
		return nil
	}

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	if err := client.PutObject(ctx, bucket, object, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("while putting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return nil
}

func SendLocalFileToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	_, err = SendLocalFileToGCSWithZstdResult(ctx, client, gsURL, localFilePath)
	return err
}

func SendLocalFileToGCSWithZstdResult(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (result SnapshotIOResult, err error) {
	ctx, span := tracer.Start(ctx, "sendLocalFileToGCSWithZstd")
	defer span.End()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening %q: %w", localFilePath, err)
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

	if fsSnapshotRoot() != "" {
		if snapshotFSZstdObjectWriteEnabled() {
			result, err := sendZstd(ctx, client, gsURL, localFile)
			if err != nil {
				return SnapshotIOResult{}, fmt.Errorf("in sendZstd object write: %w", err)
			}
			return result, nil
		}
		result, err := sendZstdToSnapshotFS(ctx, gsURL, localFile)
		if err != nil {
			return SnapshotIOResult{}, fmt.Errorf("in sendZstdToSnapshotFS: %w", err)
		}
		return result, nil
	}

	result, err = sendZstd(ctx, client, gsURL, localFile)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("in sendZstd: %w", err)
	}

	return result, nil
}

// streamingPutter marks an ObjectStorage whose PutObject accepts a non-seekable
// streaming body without buffering (e.g. GCS): implementing the interface is the
// signal, so the marker method is never called. See gcsClient.
type streamingPutter interface{ supportsStreamingPut() }

// writeContentResult reports what writeContent compressed.
type writeContentResult struct {
	// logicalBytes is the total logical size of the source, including the holes
	// for a sparse file.
	logicalBytes int64
	// populatedBytes is the count of bytes actually read + compressed: the non-hole
	// (resident) set for the sparse-extent format, == logicalBytes for a plain stream.
	populatedBytes int64
	// sparse is true when the sparse-extent format was used (the source was a file).
	sparse bool
	// sparseScanDuration is time spent discovering sparse extents. It is zero
	// for non-file streams.
	sparseScanDuration time.Duration
	// extentCopyDuration is time spent reading sparse extents from the source file
	// and feeding them into the zstd writer.
	extentCopyDuration time.Duration
}

// SnapshotIOResult is the byte-accurate evidence produced by one snapshot
// encode or decode operation. Duration measures only the encode/decode stream;
// callers separately measure storage open, sync, and publication time.
type SnapshotIOResult struct {
	LogicalBytes         int64
	PopulatedBytes       int64
	StoredBytes          int64
	WrittenBytes         int64
	Sparse               bool
	Duration             time.Duration
	SparseScanDuration   time.Duration
	ExtentCopyDuration   time.Duration
	CompressionDuration  time.Duration
	StorageWriteDuration time.Duration
	CloseDuration        time.Duration
	SyncDuration         time.Duration
	PublishDuration      time.Duration
	PrepareDuration      time.Duration
}

// SnapshotFSRawChunk describes one chunk object storing a contiguous byte range
// of a raw snapshot file. Name is relative to the snapshot directory.
type SnapshotFSRawChunk struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type countingWriter struct {
	w             io.Writer
	n             int64
	writeDuration time.Duration
}

func (w *countingWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := w.w.Write(p)
	w.writeDuration += time.Since(start)
	w.n += int64(n)
	return n, err
}

func writeContentWithResult(out io.Writer, content io.Reader) (SnapshotIOResult, error) {
	start := time.Now()
	counted := &countingWriter{w: out}
	res, err := writeContent(counted, content)
	duration := time.Since(start)
	compression := duration - res.sparseScanDuration - res.extentCopyDuration - counted.writeDuration
	if compression < 0 {
		compression = 0
	}
	return SnapshotIOResult{
		LogicalBytes:         res.logicalBytes,
		PopulatedBytes:       res.populatedBytes,
		StoredBytes:          counted.n,
		Sparse:               res.sparse,
		Duration:             duration,
		SparseScanDuration:   res.sparseScanDuration,
		ExtentCopyDuration:   res.extentCopyDuration,
		CompressionDuration:  compression,
		StorageWriteDuration: counted.writeDuration,
	}, err
}

// writeContent compresses content to out, choosing the sparse-extent format for a
// seekable *os.File (compress only the populated extents, skip the holes) or a
// plain zstd stream otherwise. It touches only io, so it is unit-testable without
// an object store, and is shared by the buffered and streaming upload paths.
func writeContent(out io.Writer, content io.Reader) (writeContentResult, error) {
	if f, ok := content.(*os.File); ok {
		logical, populated, scanDuration, extentCopyDuration, err := writeSparseZstd(out, f)
		if err != nil {
			return writeContentResult{}, err
		}
		return writeContentResult{logicalBytes: logical, populatedBytes: populated, sparse: true, sparseScanDuration: scanDuration, extentCopyDuration: extentCopyDuration}, nil
	}
	logical, err := plainZstd(out, content)
	if err != nil {
		return writeContentResult{}, err
	}
	return writeContentResult{logicalBytes: logical, populatedBytes: logical}, nil
}

// sendZstd zstd-compresses content and uploads it to gsURL.
//
// The snapshot memory-ranges is the large object here (the whole guest RAM image,
// mostly zero) on the SUSPEND critical path, so we compress with SpeedFastest across
// all CPUs — high-ratio levels scan the multi-GiB image far slower for little size
// gain on near-zero data, and the decoder auto-detects the level so restore + older
// snapshots are unaffected.
//
// Upload strategy depends on the backend:
//   - Streaming backends (GCS) accept a non-seekable body, so we pipe the compressor
//     straight into PutObject: the compress overlaps the network PUT and we never
//     stage the ~100MiB compressed payload to a temp file.
//   - S3/rustfs PutObject hands the body to the AWS SDK, which needs a seekable body
//     to sign + set Content-Length (a non-seekable pipe hangs there), so we compress
//     to a SEEKABLE temp file first.
func sendZstd(ctx context.Context, client ObjectStorage, gsURL string, content io.Reader) (SnapshotIOResult, error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while parsing URL: %w", err)
	}
	tStart := time.Now()
	if _, ok := client.(streamingPutter); ok {
		return sendStreamingZstd(ctx, client, bucket, object, content, tStart)
	}
	return sendBufferedZstd(ctx, client, bucket, object, content, tStart)
}

// sendBufferedZstd compresses content to a seekable temp file, then uploads it.
// Used for backends (S3/rustfs) whose PutObject needs a seekable body to sign and
// set Content-Length; the streaming counterpart is sendStreamingZstd.
func sendBufferedZstd(ctx context.Context, client ObjectStorage, bucket, object string, content io.Reader, tStart time.Time) (SnapshotIOResult, error) {
	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	t0 := time.Now()
	res, err := writeContentWithResult(tmpFile, content)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while compressing %q: %w", object, err)
	}
	dCompress := time.Since(t0)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while seeking temp file: %w", err)
	}
	tPut := time.Now()
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while putting object %q: %w", object, err)
	}
	res.StorageWriteDuration += time.Since(tPut)
	res.Duration = time.Since(tStart)
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", res.Sparse),
		slog.Int64("logical_bytes", res.LogicalBytes), slog.Int64("populated_bytes", res.PopulatedBytes),
		slog.Int64("compressed_bytes", res.StoredBytes),
		slog.Duration("sparse_scan", res.SparseScanDuration),
		slog.Duration("extent_copy", res.ExtentCopyDuration),
		slog.Duration("compress", dCompress), slog.Duration("write", res.StorageWriteDuration),
		slog.Duration("total", res.Duration))
	return res, nil
}

// sendStreamingZstd compresses content and uploads it in one overlapped pass: a
// goroutine writes the (sparse-extent or plain) zstd stream into an io.Pipe while
// PutObject streams the read end to the object store. No seekable temp file, and
// the compress runs concurrently with the network PUT. Used only for streaming
// backends (GCS); see sendZstd.
func sendStreamingZstd(ctx context.Context, client ObjectStorage, bucket, object string, content io.Reader, tStart time.Time) (SnapshotIOResult, error) {
	type result struct {
		res SnapshotIOResult
		err error
	}
	pr, pw := io.Pipe()
	ch := make(chan result, 1)
	go func() {
		res, err := writeContentWithResult(pw, content)
		// Closing the writer delivers EOF (or the compress error) to PutObject.
		_ = pw.CloseWithError(err)
		ch <- result{res: res, err: err}
	}()

	tPut := time.Now()
	putErr := client.PutObject(ctx, bucket, object, pr)
	dPut := time.Since(tPut)
	if putErr != nil {
		// PutObject bailed (e.g. mid-stream); unblock the compressor goroutine so it
		// can finish and we don't deadlock on the channel receive below.
		_ = pr.CloseWithError(putErr)
	}
	r := <-ch
	if putErr != nil {
		return SnapshotIOResult{}, fmt.Errorf("while putting object %q: %w", object, putErr)
	}
	if r.err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while compressing %q: %w", object, r.err)
	}
	r.res.StorageWriteDuration = maxDuration(r.res.StorageWriteDuration, dPut)
	r.res.Duration = time.Since(tStart)
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Bool("sparse", r.res.Sparse), slog.Bool("streaming", true),
		slog.Int64("logical_bytes", r.res.LogicalBytes), slog.Int64("populated_bytes", r.res.PopulatedBytes),
		slog.Int64("compressed_bytes", r.res.StoredBytes),
		slog.Duration("sparse_scan", r.res.SparseScanDuration),
		slog.Duration("extent_copy", r.res.ExtentCopyDuration),
		slog.Duration("compress", r.res.CompressionDuration),
		slog.Duration("write", r.res.StorageWriteDuration),
		slog.Duration("total", r.res.Duration))
	return r.res, nil
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// plainZstd writes src to w as a single plain zstd stream (SpeedFastest, all
// cores) and returns the uncompressed byte count.
func plainZstd(w io.Writer, src io.Reader) (int64, error) {
	zw, err := zstd.NewWriter(w,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(zw, src)
	if err != nil {
		zw.Close()
		return n, err
	}
	return n, zw.Close()
}

func FetchLocalFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "fetchLocalFileFromGCSWithZstd")
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

	if fsSnapshotRoot() != "" {
		if err := fetchZstdFromSnapshotFS(ctx, gsURL, localFile); err != nil {
			if !snapshotFSFallbackEnabled() {
				return fmt.Errorf("while fetching %q from snapshot fs: %w", gsURL, err)
			}
			slog.WarnContext(ctx, "Snapshot fs zstd read failed; falling back to object storage", slog.String("url", gsURL), slog.Any("err", err))
		} else {
			return nil
		}
	}

	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}

	return nil
}

func fetchFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, out io.Writer) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("%w:while parsing URL: %w", ateerrors.ReasonInvalidObjectURL, err)
	}

	tStart := time.Now()
	tOpen := time.Now()
	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	dOpen := time.Since(tOpen)
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err != nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	counted := &countingReader{r: rc}
	tDecode := time.Now()
	res, err := decodeContent(out, counted)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Decompressed zstd download",
		slog.String("backend", "object"), slog.String("bucket", bucket), slog.String("object", object),
		slog.Bool("sparse", res.sparse), slog.Int64("compressed_bytes", counted.n),
		slog.Int64("logical_bytes", res.logicalBytes), slog.Int64("written_bytes", res.writtenBytes),
		slog.Duration("object_open", dOpen), slog.Duration("decode_write", time.Since(tDecode)),
		slog.Duration("total", time.Since(tStart)))
	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// decodeContentResult reports what decodeContent decompressed.
type decodeContentResult struct {
	// logicalBytes is the logical size written to out (the original image size).
	logicalBytes int64
	// writtenBytes is the count of non-hole bytes actually written on the sparse
	// file path; 0 on the io.Copy fallback (non-file destination).
	writtenBytes int64
	// sparse is true when the input used the sparse-extent format.
	sparse bool
}

func decodeContentWithResult(out io.Writer, src io.Reader) (SnapshotIOResult, error) {
	start := time.Now()
	counted := &countingReader{r: src}
	res, err := decodeContent(out, counted)
	written := res.writtenBytes
	if written == 0 && !res.sparse {
		written = res.logicalBytes
	}
	return SnapshotIOResult{
		LogicalBytes: res.logicalBytes,
		StoredBytes:  counted.n,
		WrittenBytes: written,
		Sparse:       res.sparse,
		Duration:     time.Since(start),
	}, err
}

// decodeContent decompresses src into out, auto-detecting the format from the
// leading magic: the sparse-extent format (sparseMagic) vs a plain zstd stream
// (older snapshots, or the non-file upload path). When out is an *os.File the plain
// path writes SPARSE (skips zero blocks → holes) so only the resident set is
// written, not a dense multi-GiB image. It touches only io, so it is unit-testable
// without an object store, mirroring writeContent.
func decodeContent(out io.Writer, src io.Reader) (decodeContentResult, error) {
	magic := make([]byte, len(sparseMagic))
	n, rerr := io.ReadFull(src, magic)
	if rerr == nil && string(magic) == sparseMagic {
		f, ok := out.(*os.File)
		if !ok {
			return decodeContentResult{}, fmt.Errorf("sparse-extent snapshot requires a file destination, got %T", out)
		}
		size, written, derr := readSparseZstd(f, src) // src is positioned just after the magic
		if derr != nil {
			return decodeContentResult{}, fmt.Errorf("in sparse-extent decode: %w", derr)
		}
		return decodeContentResult{logicalBytes: size, writtenBytes: written, sparse: true}, nil
	}
	if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
		return decodeContentResult{}, fmt.Errorf("while reading object header: %w", rerr)
	}

	// Plain zstd stream: put back the peeked bytes, then decompress.
	r := io.MultiReader(bytes.NewReader(magic[:n]), src)
	zrc, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(snapshotDecoderConcurrency()))
	if err != nil {
		return decodeContentResult{}, fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()
	if f, ok := out.(*os.File); ok {
		size, written, derr := copyZstdSparse(f, zrc)
		if derr != nil {
			return decodeContentResult{}, fmt.Errorf("in sparse decompress: %w", derr)
		}
		return decodeContentResult{logicalBytes: size, writtenBytes: written}, nil
	}
	size, cerr := io.Copy(out, zrc)
	if cerr != nil {
		return decodeContentResult{}, fmt.Errorf("in io.Copy: %w", cerr)
	}
	return decodeContentResult{logicalBytes: size}, nil
}

// copyZstdSparse writes src into dst skipping all-zero blocks, so dst becomes a
// sparse file (the skipped regions are holes). Returns the logical size (total bytes
// consumed from src) and the bytes actually written (non-zero). dst is truncated to
// empty first (so skipped regions are real holes, not stale bytes) and to the
// logical size at the end (so trailing zero regions become a hole and the size is
// exact). dst must be a regular file opened for writing.
func copyZstdSparse(dst *os.File, src io.Reader) (size int64, written int64, err error) {
	// Start from an empty file so the holes we skip can't expose pre-existing bytes:
	// this writes out only the non-zero chunks, it does not overlay onto dst.
	if err := dst.Truncate(0); err != nil {
		return 0, 0, fmt.Errorf("truncating dst: %w", err)
	}
	// 64KiB blocks: a multiple of the 4KiB fs block (so skipped runs align to whole
	// hole-able blocks) while keeping the zero-scan + WriteAt syscall count modest.
	const block = 64 << 10
	buf := make([]byte, block)
	var pos int64
	for {
		n, rerr := io.ReadFull(src, buf)
		if n > 0 {
			chunk := buf[:n]
			nw, werr := writeNonZeroPages(dst, pos, chunk)
			written += nw
			if werr != nil {
				return 0, 0, werr
			}
			pos += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return 0, 0, rerr
		}
	}
	// Materialize the exact logical size: extends past the last written byte with a
	// hole when the tail was zero (skipped), and is a no-op otherwise.
	if terr := dst.Truncate(pos); terr != nil {
		return 0, 0, terr
	}
	return pos, written, nil
}

func writeNonZeroPages(dst *os.File, off int64, chunk []byte) (written int64, err error) {
	const page = 4 << 10
	runStart := -1
	flush := func(end int) error {
		if runStart < 0 {
			return nil
		}
		if _, werr := dst.WriteAt(chunk[runStart:end], off+int64(runStart)); werr != nil {
			return werr
		}
		written += int64(end - runStart)
		runStart = -1
		return nil
	}
	for p := 0; p < len(chunk); p += page {
		end := p + page
		if end > len(chunk) {
			end = len(chunk)
		}
		if allZero(chunk[p:end]) {
			if err := flush(p); err != nil {
				return written, err
			}
			continue
		}
		if runStart < 0 {
			runStart = p
		}
	}
	if err := flush(len(chunk)); err != nil {
		return written, err
	}
	return written, nil
}

// allZero reports whether b is all zero bytes, checking 8 bytes at a time.
func allZero(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if binary.LittleEndian.Uint64(b[i:]) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func parseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

func fsSnapshotRoot() string {
	return strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_FS_ROOT"))
}

func snapshotFSFallbackEnabled() bool {
	return strings.EqualFold(os.Getenv("ATE_SNAPSHOT_FS_FALLBACK"), "object")
}

func snapshotFSZstdDirectWriteEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_FS_ZSTD_DIRECT_WRITE"))
	return raw == "1" || strings.EqualFold(raw, "true")
}

func snapshotFSRawDirectWriteEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_FS_RAW_DIRECT_WRITE"))
	return raw == "1" || strings.EqualFold(raw, "true")
}

func snapshotFSCopyBufferBytes() int {
	const defaultBytes = 16 << 20
	raw := strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_FS_COPY_BUFFER_BYTES"))
	if raw == "" {
		return defaultBytes
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 32<<10 || n > 256<<20 {
		return defaultBytes
	}
	return n
}

type snapshotFSReader struct{ io.Reader }
type snapshotFSWriter struct{ io.Writer }

func copySnapshotFSFile(dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, snapshotFSCopyBufferBytes())
	return io.CopyBuffer(snapshotFSWriter{Writer: dst}, snapshotFSReader{Reader: src}, buf)
}

func snapshotFSZstdObjectWriteEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("ATE_SNAPSHOT_FS_ZSTD_OBJECT_WRITE"))
	return raw == "1" || strings.EqualFold(raw, "true")
}

func snapshotFSPath(gsURL string) (bucket, object, path string, err error) {
	bucket, object, err = parseGCSURL(gsURL)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	if bucket == "" {
		return "", "", "", fmt.Errorf("%w: snapshot url %q missing bucket", ateerrors.ReasonInvalidObjectURL, gsURL)
	}
	cleanObject := filepath.Clean(object)
	if cleanObject == "." || cleanObject == ".." || strings.HasPrefix(cleanObject, "../") || filepath.IsAbs(cleanObject) {
		return "", "", "", fmt.Errorf("%w: unsafe snapshot object path %q", ateerrors.ReasonInvalidObjectURL, object)
	}
	return bucket, cleanObject, filepath.Join(fsSnapshotRoot(), bucket, cleanObject), nil
}

// SnapshotFSObjectPath returns the validated local path for an object exposed
// through the configured snapshot filesystem.
func SnapshotFSObjectPath(gsURL string) (string, error) {
	if fsSnapshotRoot() == "" {
		return "", fmt.Errorf("snapshot filesystem is not configured")
	}
	_, _, path, err := snapshotFSPath(gsURL)
	return path, err
}

func ensureSnapshotFSDir(path string, perm os.FileMode) error {
	err := snapshotFSMkdirAll(path, perm)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	info, statErr := snapshotFSStat(path)
	if statErr == nil {
		if info.IsDir() {
			return nil
		}
		return err
	}
	if !errors.Is(statErr, os.ErrNotExist) {
		return err
	}
	return mkdirAllSnapshotFSSlow(path, perm)
}

func mkdirAllSnapshotFSSlow(path string, perm os.FileMode) error {
	if path == "" {
		return nil
	}
	info, err := snapshotFSStat(path)
	if err == nil {
		if info.IsDir() {
			return nil
		}
		return fmt.Errorf("%s exists and is not a directory", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if parent != path {
		if err := mkdirAllSnapshotFSSlow(parent, perm); err != nil {
			return err
		}
	}
	if err := snapshotFSMkdir(path, perm); err != nil {
		if errors.Is(err, os.ErrExist) {
			info, statErr := snapshotFSStat(path)
			if statErr != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
		}
		return err
	}
	return nil
}

// SendLocalFileToSnapshotFSRaw atomically publishes an uncompressed snapshot
// file on the snapshot filesystem. io.Copy uses the platform file-to-file fast
// path where available; the A/B benchmark records allocated bytes separately.
func SendLocalFileToSnapshotFSRaw(ctx context.Context, gsURL, localFilePath string) (SnapshotIOResult, error) {
	start := time.Now()
	_, object, target, err := snapshotFSPath(gsURL)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	if err := ensureSnapshotFSDir(filepath.Dir(target), 0o700); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while creating raw snapshot directory: %w", err)
	}
	if snapshotFSRawDirectWriteEnabled() {
		return sendRawDirectToSnapshotFS(ctx, object, target, localFilePath, start)
	}
	src, err := os.Open(localFilePath)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return SnapshotIOResult{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".substrate-raw-*")
	if err != nil {
		return SnapshotIOResult{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	n, copyErr := copySnapshotFSFile(tmp, src)
	if copyErr != nil {
		_ = tmp.Close()
		return SnapshotIOResult{}, fmt.Errorf("while copying raw snapshot %q: %w", object, copyErr)
	}
	if err := syncAndCloseSnapshotFSFile(tmp); err != nil {
		return SnapshotIOResult{}, err
	}
	if err := renameSnapshotFSFile(tmpName, target); err != nil {
		return SnapshotIOResult{}, err
	}
	result := SnapshotIOResult{LogicalBytes: info.Size(), PopulatedBytes: n, StoredBytes: n, WrittenBytes: n, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot fs write", slog.String("object", object), slog.Int64("logical_bytes", info.Size()), slog.Int64("stored_bytes", n), slog.Duration("total", result.Duration))
	return result, nil
}

func SendLocalFileToGCSRawResult(ctx context.Context, client ObjectStorage, gsURL, localFilePath string) (SnapshotIOResult, error) {
	start := time.Now()
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	src, err := os.Open(localFilePath)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return SnapshotIOResult{}, err
	}
	if err := client.PutObject(ctx, bucket, object, src); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while putting raw object bucket=%q object=%q: %w", bucket, object, err)
	}
	result := SnapshotIOResult{LogicalBytes: info.Size(), PopulatedBytes: info.Size(), StoredBytes: info.Size(), WrittenBytes: info.Size(), Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot object write", slog.String("object", object), slog.Int64("logical_bytes", info.Size()), slog.Duration("total", result.Duration))
	return result, nil
}

func SendLocalFileToGCSRawChunked(ctx context.Context, client ObjectStorage, gsURL, localFilePath string, chunkBytes int64, concurrency int) ([]SnapshotFSRawChunk, SnapshotIOResult, error) {
	start := time.Now()
	if chunkBytes <= 0 {
		return nil, SnapshotIOResult{}, fmt.Errorf("chunkBytes must be positive")
	}
	concurrency = normalizeSnapshotFSChunkConcurrency(concurrency)
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, SnapshotIOResult{}, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	src, err := os.Open(localFilePath)
	if err != nil {
		return nil, SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return nil, SnapshotIOResult{}, err
	}
	chunks := buildSnapshotFSRawChunks(path.Base(object), info.Size(), chunkBytes)
	objectDir := path.Dir(object)
	if objectDir == "." {
		objectDir = ""
	}
	if err := runSnapshotFSChunkWorkers(ctx, len(chunks), concurrency, func(i int) error {
		chunk := chunks[i]
		chunkObject := path.Join(objectDir, chunk.Name)
		reader := io.NewSectionReader(src, chunk.Offset, chunk.Size)
		if err := client.PutObject(ctx, bucket, chunkObject, reader); err != nil {
			return fmt.Errorf("while putting raw object chunk %q: %w", chunk.Name, err)
		}
		return nil
	}); err != nil {
		return nil, SnapshotIOResult{}, err
	}
	result := SnapshotIOResult{LogicalBytes: info.Size(), PopulatedBytes: info.Size(), StoredBytes: info.Size(), WrittenBytes: info.Size(), Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot object chunked write", slog.String("object", object), slog.Int("chunks", len(chunks)), slog.Int64("chunk_bytes", chunkBytes), slog.Int("concurrency", concurrency), slog.Int64("logical_bytes", info.Size()), slog.Duration("total", result.Duration))
	return chunks, result, nil
}

func FetchLocalFileFromGCSRaw(ctx context.Context, client ObjectStorage, gsURL, localFilePath string) (SnapshotIOResult, error) {
	start := time.Now()
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while getting raw object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()
	dst, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	n, copyErr := io.CopyBuffer(dst, rc, make([]byte, snapshotFSCopyBufferBytes()))
	closeErr := dst.Close()
	if copyErr != nil {
		return SnapshotIOResult{}, copyErr
	}
	if closeErr != nil {
		return SnapshotIOResult{}, closeErr
	}
	result := SnapshotIOResult{LogicalBytes: n, StoredBytes: n, WrittenBytes: n, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot object read", slog.String("object", object), slog.Int64("bytes", n), slog.Duration("total", result.Duration))
	return result, nil
}

func FetchLocalFileFromGCSRawChunked(ctx context.Context, client ObjectStorage, gsURL, localFilePath string, chunks []SnapshotFSRawChunk, concurrency int) (SnapshotIOResult, error) {
	start := time.Now()
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("%w: while parsing url: %w", ateerrors.ReasonInvalidObjectURL, err)
	}
	concurrency = normalizeSnapshotFSChunkConcurrency(concurrency)
	totalBytes := int64(0)
	for _, chunk := range chunks {
		if chunk.Offset < 0 || chunk.Size < 0 {
			return SnapshotIOResult{}, fmt.Errorf("invalid raw snapshot chunk %q: offset=%d size=%d", chunk.Name, chunk.Offset, chunk.Size)
		}
		totalBytes = max(totalBytes, chunk.Offset+chunk.Size)
	}
	dst, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	if err := dst.Truncate(totalBytes); err != nil {
		_ = dst.Close()
		return SnapshotIOResult{}, err
	}
	objectDir := path.Dir(object)
	if objectDir == "." {
		objectDir = ""
	}
	if err := runSnapshotFSChunkWorkers(ctx, len(chunks), concurrency, func(i int) error {
		chunk := chunks[i]
		if path.Base(path.Dir(chunk.Name)) != path.Base(object)+".parts" || path.Base(chunk.Name) == "." {
			return fmt.Errorf("unsafe raw snapshot chunk name %q", chunk.Name)
		}
		chunkObject := path.Join(objectDir, chunk.Name)
		rc, err := client.GetObject(ctx, bucket, chunkObject)
		if err != nil {
			return fmt.Errorf("while getting raw object chunk %q: %w", chunk.Name, err)
		}
		writer := &writerAtSection{w: dst, off: chunk.Offset}
		n, copyErr := copySnapshotFSFile(writer, io.LimitReader(rc, chunk.Size))
		closeErr := rc.Close()
		if copyErr != nil {
			return fmt.Errorf("while reading raw object chunk %q: %w", chunk.Name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("while closing raw object chunk %q: %w", chunk.Name, closeErr)
		}
		if n != chunk.Size {
			return fmt.Errorf("short raw object chunk read %q: read %d bytes, want %d", chunk.Name, n, chunk.Size)
		}
		return nil
	}); err != nil {
		_ = dst.Close()
		return SnapshotIOResult{}, err
	}
	if err := dst.Close(); err != nil {
		return SnapshotIOResult{}, err
	}
	result := SnapshotIOResult{LogicalBytes: totalBytes, StoredBytes: totalBytes, WrittenBytes: totalBytes, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot object chunked read", slog.String("object", object), slog.Int("chunks", len(chunks)), slog.Int("concurrency", concurrency), slog.Int64("bytes", totalBytes), slog.Duration("total", result.Duration))
	return result, nil
}

func sendRawDirectToSnapshotFS(ctx context.Context, object, target, localFilePath string, start time.Time) (SnapshotIOResult, error) {
	src, err := os.Open(localFilePath)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return SnapshotIOResult{}, err
	}
	dst, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot target: %w", err)
	}
	n, copyErr := copySnapshotFSFile(dst, src)
	closeErr := dst.Close()
	if copyErr != nil {
		_ = os.Remove(target)
		return SnapshotIOResult{}, fmt.Errorf("while copying raw snapshot %q: %w", object, copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(target)
		return SnapshotIOResult{}, fmt.Errorf("while closing raw snapshot target: %w", closeErr)
	}
	result := SnapshotIOResult{LogicalBytes: info.Size(), PopulatedBytes: n, StoredBytes: n, WrittenBytes: n, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot fs write", slog.String("object", object), slog.String("publish_mode", "direct"), slog.Int64("logical_bytes", info.Size()), slog.Int64("stored_bytes", n), slog.Duration("total", result.Duration))
	return result, nil
}

// SendLocalFileToSnapshotFSRawChunked publishes a raw snapshot as independent
// chunk files under <object>.parts/, allowing Alluxio/FUSE to handle multiple
// file streams in parallel.
func SendLocalFileToSnapshotFSRawChunked(ctx context.Context, gsURL, localFilePath string, chunkBytes int64, concurrency int) ([]SnapshotFSRawChunk, SnapshotIOResult, error) {
	start := time.Now()
	if chunkBytes <= 0 {
		return nil, SnapshotIOResult{}, fmt.Errorf("chunkBytes must be positive")
	}
	concurrency = normalizeSnapshotFSChunkConcurrency(concurrency)
	_, object, target, err := snapshotFSPath(gsURL)
	if err != nil {
		return nil, SnapshotIOResult{}, err
	}
	src, err := os.Open(localFilePath)
	if err != nil {
		return nil, SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot source: %w", err)
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return nil, SnapshotIOResult{}, err
	}
	partsDir := target + ".parts"
	if err := os.RemoveAll(partsDir); err != nil {
		return nil, SnapshotIOResult{}, fmt.Errorf("while removing previous raw snapshot chunks: %w", err)
	}
	if err := ensureSnapshotFSDir(partsDir, 0o700); err != nil {
		return nil, SnapshotIOResult{}, fmt.Errorf("while creating raw snapshot chunk directory: %w", err)
	}
	chunks := buildSnapshotFSRawChunks(filepath.Base(object), info.Size(), chunkBytes)
	if err := runSnapshotFSChunkWorkers(ctx, len(chunks), concurrency, func(i int) error {
		chunk := chunks[i]
		dstPath := filepath.Join(partsDir, filepath.Base(chunk.Name))
		dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("while opening raw snapshot chunk %q: %w", chunk.Name, err)
		}
		reader := io.NewSectionReader(src, chunk.Offset, chunk.Size)
		n, copyErr := copySnapshotFSFile(dst, reader)
		closeErr := dst.Close()
		if copyErr != nil {
			_ = os.Remove(dstPath)
			return fmt.Errorf("while copying raw snapshot chunk %q: %w", chunk.Name, copyErr)
		}
		if closeErr != nil {
			_ = os.Remove(dstPath)
			return fmt.Errorf("while closing raw snapshot chunk %q: %w", chunk.Name, closeErr)
		}
		if n != chunk.Size {
			_ = os.Remove(dstPath)
			return fmt.Errorf("short raw snapshot chunk write %q: wrote %d bytes, want %d", chunk.Name, n, chunk.Size)
		}
		return nil
	}); err != nil {
		return nil, SnapshotIOResult{}, err
	}
	result := SnapshotIOResult{LogicalBytes: info.Size(), PopulatedBytes: info.Size(), StoredBytes: info.Size(), WrittenBytes: info.Size(), Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot fs chunked write", slog.String("object", object), slog.Int("chunks", len(chunks)), slog.Int64("chunk_bytes", chunkBytes), slog.Int("concurrency", concurrency), slog.Int64("logical_bytes", info.Size()), slog.Duration("total", result.Duration))
	return chunks, result, nil
}

func buildSnapshotFSRawChunks(baseName string, size, chunkBytes int64) []SnapshotFSRawChunk {
	if size == 0 {
		return nil
	}
	chunks := make([]SnapshotFSRawChunk, 0, (size+chunkBytes-1)/chunkBytes)
	for offset := int64(0); offset < size; offset += chunkBytes {
		chunkSize := min(chunkBytes, size-offset)
		chunks = append(chunks, SnapshotFSRawChunk{
			Name:   filepath.ToSlash(filepath.Join(baseName+".parts", fmt.Sprintf("%08d", len(chunks)))),
			Offset: offset,
			Size:   chunkSize,
		})
	}
	return chunks
}

func normalizeSnapshotFSChunkConcurrency(concurrency int) int {
	if concurrency < 1 {
		return 1
	}
	if concurrency > 64 {
		return 64
	}
	return concurrency
}

// FetchLocalFileFromSnapshotFSRaw materializes a raw snapshot file locally.
func FetchLocalFileFromSnapshotFSRaw(ctx context.Context, gsURL, localFilePath string) (SnapshotIOResult, error) {
	start := time.Now()
	_, object, source, err := snapshotFSPath(gsURL)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	src, err := os.Open(source)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening raw snapshot %q: %w", object, err)
	}
	defer src.Close()
	dst, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	n, copyErr := copySnapshotFSFile(dst, src)
	closeErr := dst.Close()
	if copyErr != nil {
		return SnapshotIOResult{}, copyErr
	}
	if closeErr != nil {
		return SnapshotIOResult{}, closeErr
	}
	result := SnapshotIOResult{LogicalBytes: n, StoredBytes: n, WrittenBytes: n, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot fs read", slog.String("object", object), slog.Int64("bytes", n), slog.Duration("total", result.Duration))
	return result, nil
}

// FetchLocalFileFromSnapshotFSRawChunked materializes a chunked raw snapshot
// back to the single checkpoint file expected by runsc.
func FetchLocalFileFromSnapshotFSRawChunked(ctx context.Context, gsURL, localFilePath string, chunks []SnapshotFSRawChunk, concurrency int) (SnapshotIOResult, error) {
	start := time.Now()
	_, object, source, err := snapshotFSPath(gsURL)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	concurrency = normalizeSnapshotFSChunkConcurrency(concurrency)
	totalBytes := int64(0)
	for _, chunk := range chunks {
		if chunk.Offset < 0 || chunk.Size < 0 {
			return SnapshotIOResult{}, fmt.Errorf("invalid raw snapshot chunk %q: offset=%d size=%d", chunk.Name, chunk.Offset, chunk.Size)
		}
		totalBytes = max(totalBytes, chunk.Offset+chunk.Size)
	}
	dst, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	if err := dst.Truncate(totalBytes); err != nil {
		_ = dst.Close()
		return SnapshotIOResult{}, err
	}
	if err := runSnapshotFSChunkWorkers(ctx, len(chunks), concurrency, func(i int) error {
		chunk := chunks[i]
		if filepath.Base(filepath.Dir(chunk.Name)) != filepath.Base(object)+".parts" || filepath.Base(chunk.Name) == "." {
			return fmt.Errorf("unsafe raw snapshot chunk name %q", chunk.Name)
		}
		chunkPath := filepath.Join(source+".parts", filepath.Base(chunk.Name))
		src, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("while opening raw snapshot chunk %q: %w", chunk.Name, err)
		}
		writer := &writerAtSection{w: dst, off: chunk.Offset}
		n, copyErr := copySnapshotFSFile(writer, io.LimitReader(src, chunk.Size))
		closeErr := src.Close()
		if copyErr != nil {
			return fmt.Errorf("while reading raw snapshot chunk %q: %w", chunk.Name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("while closing raw snapshot chunk %q: %w", chunk.Name, closeErr)
		}
		if n != chunk.Size {
			return fmt.Errorf("short raw snapshot chunk read %q: read %d bytes, want %d", chunk.Name, n, chunk.Size)
		}
		return nil
	}); err != nil {
		_ = dst.Close()
		return SnapshotIOResult{}, err
	}
	if err := dst.Close(); err != nil {
		return SnapshotIOResult{}, err
	}
	result := SnapshotIOResult{LogicalBytes: totalBytes, StoredBytes: totalBytes, WrittenBytes: totalBytes, Sparse: true, Duration: time.Since(start)}
	slog.InfoContext(ctx, "Raw snapshot fs chunked read", slog.String("object", object), slog.Int("chunks", len(chunks)), slog.Int("concurrency", concurrency), slog.Int64("bytes", totalBytes), slog.Duration("total", result.Duration))
	return result, nil
}

type writerAtSection struct {
	w   io.WriterAt
	off int64
}

func (w *writerAtSection) Write(p []byte) (int, error) {
	n, err := w.w.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

func runSnapshotFSChunkWorkers(ctx context.Context, count, concurrency int, fn func(int) error) error {
	if count == 0 {
		return nil
	}
	concurrency = min(normalizeSnapshotFSChunkConcurrency(concurrency), count)
	jobs := make(chan int)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					select {
					case errCh <- ctx.Err():
					default:
					}
					continue
				}
				if err := fn(i); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}
		}()
	}
	for i := range count {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func fetchBytesFromSnapshotFS(ctx context.Context, gsURL string) ([]byte, error) {
	bucket, object, path, err := snapshotFSPath(gsURL)
	if err != nil {
		return nil, err
	}
	tStart := time.Now()
	var content []byte
	retries, err := retrySnapshotFSRead(ctx, bucket, object, path, func() error {
		var readErr error
		content, readErr = os.ReadFile(path)
		return readErr
	})
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot fs file %q: %w", path, err)
	}
	slog.InfoContext(ctx, "Snapshot fs read",
		slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
		slog.Int("bytes", len(content)), slog.Int("retries", retries), slog.Duration("total", time.Since(tStart)))
	return content, nil
}

func sendBytesToSnapshotFS(ctx context.Context, gsURL string, content []byte) error {
	bucket, object, path, err := snapshotFSPath(gsURL)
	if err != nil {
		return err
	}
	tStart := time.Now()
	if err := ensureSnapshotFSDir(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("while creating snapshot fs dir for %q: %w", path, err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".substrate-manifest-*")
	if err != nil {
		return fmt.Errorf("while creating temp snapshot fs file: %w", err)
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("while writing temp snapshot fs file: %w", err)
	}
	if err := syncAndCloseSnapshotFSFile(tmp); err != nil {
		return fmt.Errorf("while syncing temp snapshot fs file: %w", err)
	}
	if err := renameSnapshotFSFile(tmpName, path); err != nil {
		return fmt.Errorf("while renaming temp snapshot fs file: %w", err)
	}
	removeTmp = false
	slog.InfoContext(ctx, "Snapshot fs write",
		slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
		slog.Int("bytes", len(content)), slog.Duration("total", time.Since(tStart)))
	return nil
}

func sendZstdToSnapshotFS(ctx context.Context, gsURL string, content io.Reader) (SnapshotIOResult, error) {
	bucket, object, path, err := snapshotFSPath(gsURL)
	if err != nil {
		return SnapshotIOResult{}, err
	}
	tStart := time.Now()
	if err := ensureSnapshotFSDir(filepath.Dir(path), 0o700); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while creating snapshot fs dir for %q: %w", path, err)
	}
	if snapshotFSZstdDirectWriteEnabled() {
		return sendZstdDirectToSnapshotFS(ctx, bucket, object, path, content, tStart)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".substrate-zstd-*")
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while creating temp snapshot fs zstd file: %w", err)
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()
	prepareDuration := time.Since(tStart)
	tCompress := time.Now()
	res, err := writeContentWithResult(tmp, content)
	if err != nil {
		_ = tmp.Close()
		return SnapshotIOResult{}, fmt.Errorf("while compressing snapshot fs file %q: %w", object, err)
	}
	dCompress := time.Since(tCompress)
	tSync := time.Now()
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SnapshotIOResult{}, fmt.Errorf("while syncing snapshot fs zstd temp file: %w", err)
	}
	res.SyncDuration = time.Since(tSync)
	tClose := time.Now()
	if err := tmp.Close(); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while closing snapshot fs zstd temp file: %w", err)
	}
	res.CloseDuration = time.Since(tClose)
	res.PrepareDuration = prepareDuration
	tRename := time.Now()
	if err := renameSnapshotFSFile(tmpName, path); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while renaming snapshot fs zstd temp file: %w", err)
	}
	removeTmp = false
	renameDuration := time.Since(tRename)
	res.PublishDuration = renameDuration
	res.Duration = time.Since(tStart)
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
		slog.Bool("sparse", res.Sparse), slog.Int64("logical_bytes", res.LogicalBytes),
		slog.Int64("populated_bytes", res.PopulatedBytes), slog.Int64("compressed_bytes", res.StoredBytes),
		slog.Duration("sparse_scan", res.SparseScanDuration),
		slog.Duration("extent_copy", res.ExtentCopyDuration),
		slog.Duration("prepare", res.PrepareDuration),
		slog.Duration("compress", dCompress), slog.Duration("write", res.StorageWriteDuration),
		slog.Duration("sync", res.SyncDuration), slog.Duration("close", res.CloseDuration),
		slog.Duration("publish", res.PublishDuration), slog.Duration("total", res.Duration))
	return res, nil
}

func sendZstdDirectToSnapshotFS(ctx context.Context, bucket, object, path string, content io.Reader, tStart time.Time) (SnapshotIOResult, error) {
	tmp, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while opening snapshot fs zstd file: %w", err)
	}
	removePartial := true
	defer func() {
		if removePartial {
			_ = os.Remove(path)
		}
	}()
	prepareDuration := time.Since(tStart)
	tCompress := time.Now()
	res, err := writeContentWithResult(tmp, content)
	if err != nil {
		_ = tmp.Close()
		return SnapshotIOResult{}, fmt.Errorf("while compressing snapshot fs file %q: %w", object, err)
	}
	dCompress := time.Since(tCompress)
	tSync := time.Now()
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SnapshotIOResult{}, fmt.Errorf("while syncing snapshot fs zstd file: %w", err)
	}
	res.SyncDuration = time.Since(tSync)
	tClose := time.Now()
	if err := tmp.Close(); err != nil {
		return SnapshotIOResult{}, fmt.Errorf("while closing snapshot fs zstd file: %w", err)
	}
	res.CloseDuration = time.Since(tClose)
	res.PrepareDuration = prepareDuration
	res.Duration = time.Since(tStart)
	removePartial = false
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
		slog.String("publish_mode", "direct"), slog.Bool("sparse", res.Sparse),
		slog.Int64("logical_bytes", res.LogicalBytes),
		slog.Int64("populated_bytes", res.PopulatedBytes), slog.Int64("compressed_bytes", res.StoredBytes),
		slog.Duration("sparse_scan", res.SparseScanDuration),
		slog.Duration("extent_copy", res.ExtentCopyDuration),
		slog.Duration("prepare", res.PrepareDuration),
		slog.Duration("compress", dCompress), slog.Duration("write", res.StorageWriteDuration),
		slog.Duration("sync", res.SyncDuration), slog.Duration("close", res.CloseDuration),
		slog.Duration("publish", res.PublishDuration), slog.Duration("total", res.Duration))
	return res, nil
}

func syncAndCloseSnapshotFSFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func renameSnapshotFSFile(src, dst string) error {
	var lastErr error
	for attempt := 0; attempt <= len(snapshotFSRenameRetryDelays); attempt++ {
		err := snapshotFSRename(src, dst)
		if err == nil {
			return nil
		}
		lastErr = err
		if !errors.Is(err, os.ErrNotExist) || attempt == len(snapshotFSRenameRetryDelays) {
			break
		}
		time.Sleep(snapshotFSRenameRetryDelays[attempt])
	}
	return lastErr
}

func fetchZstdFromSnapshotFS(ctx context.Context, gsURL string, out io.Writer) error {
	bucket, object, path, err := snapshotFSPath(gsURL)
	if err != nil {
		return err
	}
	tStart := time.Now()
	tOpen := time.Now()
	var f *os.File
	retries, err := retrySnapshotFSRead(ctx, bucket, object, path, func() error {
		var openErr error
		f, openErr = os.Open(path)
		return openErr
	})
	if err != nil {
		return fmt.Errorf("while opening snapshot fs zstd file %q: %w", path, err)
	}
	dOpen := time.Since(tOpen)
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("while stating snapshot fs zstd file %q: %w", path, err)
	}
	counted := &countingReader{r: f}
	tDecode := time.Now()
	res, err := decodeContent(out, counted)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Decompressed zstd download",
		slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
		slog.Bool("sparse", res.sparse), slog.Int64("compressed_bytes", counted.n),
		slog.Int64("compressed_file_bytes", fi.Size()), slog.Int64("logical_bytes", res.logicalBytes),
		slog.Int64("written_bytes", res.writtenBytes), slog.Duration("open", dOpen), slog.Int("retries", retries),
		slog.Duration("decode_write", time.Since(tDecode)), slog.Duration("total", time.Since(tStart)))
	return nil
}

func retrySnapshotFSRead(ctx context.Context, bucket, object, path string, op func() error) (int, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return attempt, nil
		}
		if !isSnapshotFSReadRetryable(lastErr) || attempt >= len(snapshotFSReadRetryDelays) {
			return attempt, lastErr
		}
		delay := snapshotFSReadRetryDelays[attempt]
		slog.WarnContext(ctx, "Snapshot fs read not visible yet; retrying",
			slog.String("backend", "snapshot_fs"), slog.String("bucket", bucket), slog.String("object", object),
			slog.String("path", path), slog.Int("attempt", attempt+1), slog.Duration("delay", delay), slog.Any("err", lastErr))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return attempt, ctx.Err()
		case <-timer.C:
		}
	}
}

func isSnapshotFSReadRetryable(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
