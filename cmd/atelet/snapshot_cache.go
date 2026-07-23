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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

const snapshotCacheManifestName = "manifest.json"

var snapshotCacheMu sync.Mutex

type snapshotCache struct {
	root       string
	actorsRoot string
	maxBytes   int64
}

func newSnapshotCache(root, actorsRoot string, maxBytes int64) *snapshotCache {
	return &snapshotCache{root: root, actorsRoot: actorsRoot, maxBytes: maxBytes}
}

func configuredSnapshotCache() *snapshotCache {
	maxBytes, err := strconv.ParseInt(os.Getenv("ATE_SNAPSHOT_CACHE_MAX_BYTES"), 10, 64)
	if err != nil || maxBytes <= 0 {
		return nil
	}
	return newSnapshotCache(ateompath.SnapshotCacheDir(), ateompath.ActorsDir(), maxBytes)
}

func (c *snapshotCache) entryPath(uri string) string {
	sum := sha256.Sum256([]byte(strings.TrimSuffix(uri, "/")))
	return filepath.Join(c.root, hex.EncodeToString(sum[:]))
}

func validateSnapshotCacheFiles(files []string) error {
	if len(files) == 0 {
		return errors.New("snapshot cache requires at least one file")
	}
	for _, name := range files {
		if name == "" || name == "." || filepath.Base(name) != name || name == snapshotCacheManifestName {
			return fmt.Errorf("unsafe snapshot cache file %q", name)
		}
	}
	return nil
}

// publish atomically moves decoded checkpoint files into an immutable cache
// entry. The source and cache must be on the same filesystem; this keeps cache
// publication off the large-file copy path.
func (c *snapshotCache) publish(uri, sourceDir string, files []string, manifest []byte) (bool, error) {
	snapshotCacheMu.Lock()
	defer snapshotCacheMu.Unlock()
	if err := validateSnapshotCacheFiles(files); err != nil {
		return false, err
	}
	if c.maxBytes <= 0 {
		return false, nil
	}
	if err := os.MkdirAll(c.root, 0o700); err != nil {
		return false, fmt.Errorf("while creating snapshot cache root: %w", err)
	}
	final := c.entryPath(uri)
	if _, hit, err := c.lookup(uri, files, manifest); err != nil {
		return false, err
	} else if hit {
		return true, nil
	}

	tmp, err := os.MkdirTemp(c.root, ".publish-")
	if err != nil {
		return false, fmt.Errorf("while creating snapshot cache staging directory: %w", err)
	}
	keepTmp := false
	defer func() {
		if !keepTmp {
			_ = os.RemoveAll(tmp)
		}
	}()

	var incoming int64
	for _, name := range files {
		src := filepath.Join(sourceDir, name)
		info, err := os.Stat(src)
		if err != nil {
			return false, fmt.Errorf("while stating snapshot cache source %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("snapshot cache source %q is not a regular file", name)
		}
		incoming += info.Size()
		if err := os.Rename(src, filepath.Join(tmp, name)); err != nil {
			return false, fmt.Errorf("while moving snapshot cache source %q: %w", name, err)
		}
	}
	if err := writeFileAtomic(filepath.Join(tmp, snapshotCacheManifestName), manifest, 0o600); err != nil {
		return false, fmt.Errorf("while writing snapshot cache manifest: %w", err)
	}

	if ok, err := c.makeRoom(incoming); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	if err := syncDirectory(tmp); err != nil {
		return false, fmt.Errorf("while syncing snapshot cache entry: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		if _, hit, lookupErr := c.lookup(uri, files, manifest); lookupErr == nil && hit {
			return true, nil
		}
		return false, fmt.Errorf("while publishing snapshot cache entry: %w", err)
	}
	keepTmp = true
	if err := syncDirectory(c.root); err != nil {
		return false, fmt.Errorf("while syncing snapshot cache root: %w", err)
	}
	return true, nil
}

// linkRestore atomically turns restoreDir into a reference to a validated
// immutable cache entry. Holding the cache lock across lookup and symlink
// publication prevents a concurrent eviction from opening a race window.
func (c *snapshotCache) linkRestore(uri, restoreDir string, files []string, manifest []byte) (bool, error) {
	snapshotCacheMu.Lock()
	defer snapshotCacheMu.Unlock()
	entry, hit, err := c.lookup(uri, files, manifest)
	if err != nil || !hit {
		return false, err
	}
	if err := os.RemoveAll(restoreDir); err != nil {
		return false, fmt.Errorf("while replacing restore directory with snapshot cache entry: %w", err)
	}
	if err := os.Symlink(entry, restoreDir); err != nil {
		if mkdirErr := os.MkdirAll(restoreDir, 0o700); mkdirErr != nil {
			return false, fmt.Errorf("while linking snapshot cache entry: %w (also failed to restore directory: %v)", err, mkdirErr)
		}
		return false, fmt.Errorf("while linking snapshot cache entry: %w", err)
	}
	return true, nil
}

func (c *snapshotCache) lookup(uri string, files []string, manifest []byte) (string, bool, error) {
	if err := validateSnapshotCacheFiles(files); err != nil {
		return "", false, err
	}
	entry := c.entryPath(uri)
	gotManifest, err := os.ReadFile(filepath.Join(entry, snapshotCacheManifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("while reading snapshot cache manifest: %w", err)
	}
	if !bytes.Equal(gotManifest, manifest) {
		return "", false, nil
	}
	for _, name := range files {
		info, err := os.Stat(filepath.Join(entry, name))
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		if err != nil {
			return "", false, fmt.Errorf("while validating cached snapshot file %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return "", false, nil
		}
	}
	return entry, true, nil
}

type snapshotCacheEntry struct {
	path    string
	size    int64
	modTime int64
}

func (c *snapshotCache) makeRoom(incoming int64) (bool, error) {
	if incoming > c.maxBytes {
		return false, nil
	}
	entries, total, err := c.entries()
	if err != nil {
		return false, err
	}
	if total+incoming <= c.maxBytes {
		return true, nil
	}
	referenced, err := c.referencedEntries()
	if err != nil {
		return false, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].modTime < entries[j].modTime })
	for _, entry := range entries {
		if referenced[entry.path] {
			continue
		}
		if err := os.RemoveAll(entry.path); err != nil {
			return false, fmt.Errorf("while evicting snapshot cache entry: %w", err)
		}
		total -= entry.size
		if total+incoming <= c.maxBytes {
			return true, nil
		}
	}
	return false, nil
}

func (c *snapshotCache) entries() ([]snapshotCacheEntry, int64, error) {
	dirs, err := os.ReadDir(c.root)
	if err != nil {
		return nil, 0, err
	}
	var entries []snapshotCacheEntry
	var total int64
	for _, dir := range dirs {
		if !dir.IsDir() || strings.HasPrefix(dir.Name(), ".") {
			continue
		}
		path := filepath.Join(c.root, dir.Name())
		info, err := dir.Info()
		if err != nil {
			return nil, 0, err
		}
		var size int64
		if err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.Type().IsRegular() || d.Name() == snapshotCacheManifestName {
				return nil
			}
			fileInfo, err := d.Info()
			if err != nil {
				return err
			}
			size += fileInfo.Size()
			return nil
		}); err != nil {
			return nil, 0, err
		}
		entries = append(entries, snapshotCacheEntry{path: path, size: size, modTime: info.ModTime().UnixNano()})
		total += size
	}
	return entries, total, nil
}

func (c *snapshotCache) referencedEntries() (map[string]bool, error) {
	refs := make(map[string]bool)
	resolvedRoot, err := filepath.EvalSymlinks(c.root)
	if errors.Is(err, fs.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while resolving snapshot cache root: %w", err)
	}
	actors, err := os.ReadDir(c.actorsRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while scanning snapshot cache references: %w", err)
	}
	for _, actor := range actors {
		if !actor.IsDir() {
			continue
		}
		path := filepath.Join(c.actorsRoot, actor.Name(), "restore-state")
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			continue
		}
		target, err := filepath.EvalSymlinks(path)
		if err != nil {
			continue
		}
		if rel, err := filepath.Rel(resolvedRoot, target); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			refs[filepath.Join(c.root, filepath.Base(target))] = true
		}
	}
	return refs, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
