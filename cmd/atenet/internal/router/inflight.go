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

package router

import (
	"context"
	"sync"
)

type inFlightTracker struct {
	mu     sync.Mutex
	counts map[string]int
	waitCh map[string]chan struct{}
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{
		counts: map[string]int{},
		waitCh: map[string]chan struct{}{},
	}
}

func (t *inFlightTracker) begin(key string) func() {
	t.mu.Lock()
	t.counts[key]++
	if t.waitCh[key] == nil {
		t.waitCh[key] = make(chan struct{})
	}
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			t.end(key)
		})
	}
}

func (t *inFlightTracker) end(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.counts[key] == 0 {
		return
	}

	t.counts[key]--
	if t.counts[key] != 0 {
		return
	}

	if ch := t.waitCh[key]; ch != nil {
		close(ch)
	}
	delete(t.counts, key)
	delete(t.waitCh, key)
}

func (t *inFlightTracker) waitZero(ctx context.Context, key string) error {
	t.mu.Lock()
	if t.counts[key] == 0 {
		t.mu.Unlock()
		return nil
	}
	ch := t.waitCh[key]
	t.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
