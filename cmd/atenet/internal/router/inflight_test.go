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
	"testing"
	"time"
)

func TestInFlightTrackerWaitsForDone(t *testing.T) {
	tracker := newInFlightTracker()
	done := tracker.begin("space/actor")

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- tracker.waitZero(context.Background(), "space/actor")
	}()

	time.Sleep(20 * time.Millisecond)
	done()

	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitZero timed out")
	}
}

func TestInFlightTrackerHonorsContext(t *testing.T) {
	tracker := newInFlightTracker()
	_ = tracker.begin("space/actor")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := tracker.waitZero(ctx, "space/actor"); err == nil {
		t.Fatal("expected context timeout")
	}
}
