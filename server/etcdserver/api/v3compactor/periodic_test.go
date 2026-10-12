// Copyright 2015 The etcd Authors
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

package v3compactor

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
)

func TestPeriodicHourly(t *testing.T) {
	retentionHours := 2
	retentionDuration := time.Duration(retentionHours) * time.Hour

	fc := clockwork.NewFakeClock()
	// TODO: Do not depand or real time (Recorder.Wait) in unit tests.
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(10 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, 0, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// compaction doesn't happen til 2 hours elapse
	for i := 0; i < initialIntervals-1; i++ {
		waitOneAction(t, rg)
		fc.Advance(tb.getRetryInterval())
	}

	// very first compaction
	a, err := waitWithRetry(t, compactable)
	if err != nil {
		t.Fatal(err)
	}
	expectedRevision := int64(1)
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}

	// simulate 3 hours
	// now compactor kicks in, every hour
	for i := 0; i < 3; i++ {
		// advance one hour, one revision for each interval
		for j := 0; j < intervalsPerPeriod; j++ {
			waitOneAction(t, rg)
			fc.Advance(tb.getRetryInterval())
		}

		a, err = waitWithRetry(t, compactable)
		if err != nil {
			t.Fatal(err)
		}

		expectedRevision = int64((i + 1) * 10)
		if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
			t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
		}
	}
}

func TestPeriodicMinutes(t *testing.T) {
	retentionMinutes := 5
	retentionDuration := time.Duration(retentionMinutes) * time.Minute

	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(10 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, 0, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// compaction doesn't happen til 5 minutes elapse
	for i := 0; i < initialIntervals-1; i++ {
		waitOneAction(t, rg)
		fc.Advance(tb.getRetryInterval())
	}

	// very first compaction
	a, err := waitWithRetry(t, compactable)
	if err != nil {
		t.Fatal(err)
	}
	expectedRevision := int64(1)
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}

	// compaction happens at every interval
	for i := 0; i < 5; i++ {
		// advance 5-minute, one revision for each interval
		for j := 0; j < intervalsPerPeriod; j++ {
			waitOneAction(t, rg)
			fc.Advance(tb.getRetryInterval())
		}

		a, err := waitWithRetry(t, compactable)
		if err != nil {
			t.Fatal(err)
		}

		expectedRevision = int64((i + 1) * 10)
		if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
			t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
		}
	}
}

// TestPeriodicMinutesWithInterval checks that a configured interval sets how
// often compaction runs after the first one, which still waits for one
// retention period.
func TestPeriodicMinutesWithInterval(t *testing.T) {
	retentionDuration := 10 * time.Minute
	intervalDuration := 2 * time.Minute

	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(10 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, intervalDuration, rg, compactable)

	tb.Run()
	defer tb.Stop()

	retryInterval := tb.getRetryInterval() // a tenth of the retention: 1 minute
	checksPerInterval := int(intervalDuration / retryInterval)

	// compaction doesn't happen til 10 minutes elapse
	for i := 0; i < tb.getRetentions()-1; i++ {
		waitOneAction(t, rg)
		fc.Advance(retryInterval)
	}

	// very first compaction
	a, err := waitWithRetry(t, compactable)
	require.NoError(t, err)
	require.Equal(t, &pb.CompactionRequest{Revision: 1}, a[0].Params[0])

	// compaction happens every interval and keeps 10 minutes of revisions
	for i := 0; i < 5; i++ {
		for j := 0; j < checksPerInterval; j++ {
			waitOneAction(t, rg)
			fc.Advance(retryInterval)
		}

		a, err := waitWithRetry(t, compactable)
		require.NoError(t, err)
		require.Equal(t, &pb.CompactionRequest{Revision: int64((i + 1) * checksPerInterval)}, a[0].Params[0])
	}
}

// TestPeriodicIntervalShorterThanRetryInterval checks that an interval shorter
// than a tenth of the retention takes effect, instead of being rounded up to
// how often the compactor checks.
func TestPeriodicIntervalShorterThanRetryInterval(t *testing.T) {
	retentionDuration := time.Hour // checked every 6 minutes without an interval
	intervalDuration := time.Minute

	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(10 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, intervalDuration, rg, compactable)
	require.Equal(t, intervalDuration, tb.getRetryInterval())

	tb.Run()
	defer tb.Stop()

	// compaction doesn't happen til 1 hour elapses
	for i := 0; i < tb.getRetentions()-1; i++ {
		waitOneAction(t, rg)
		fc.Advance(intervalDuration)
	}

	// very first compaction
	a, err := waitWithRetry(t, compactable)
	require.NoError(t, err)
	require.Equal(t, &pb.CompactionRequest{Revision: 1}, a[0].Params[0])

	// the next check finds the same oldest revision and skips
	waitOneAction(t, rg)
	fc.Advance(intervalDuration)

	// after that, compaction happens every minute
	for wantRevision := int64(2); wantRevision <= 6; wantRevision++ {
		waitOneAction(t, rg)
		fc.Advance(intervalDuration)

		a, err := waitWithRetry(t, compactable)
		require.NoError(t, err)
		require.Equal(t, &pb.CompactionRequest{Revision: wantRevision}, a[0].Params[0])
	}
}

func TestPeriodicPause(t *testing.T) {
	fc := clockwork.NewFakeClock()
	retentionDuration := time.Hour
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(10 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, 0, rg, compactable)

	tb.Run()
	tb.Pause()

	n := tb.getRetentions()

	// tb will collect 3 hours of revisions but not compact since paused
	for i := 0; i < n*3; i++ {
		waitOneAction(t, rg)
		fc.Advance(tb.getRetryInterval())
	}
	// t.revs = [21 22 23 24 25 26 27 28 29 30]

	select {
	case a := <-compactable.Chan():
		t.Fatalf("unexpected action %v", a)
	case <-time.After(10 * time.Millisecond):
	}

	// tb resumes to being blocked on the clock
	tb.Resume()
	waitOneAction(t, rg)

	// unblock clock, will kick off a compaction at T=3h6m by retry
	fc.Advance(tb.getRetryInterval())

	// T=3h6m
	a, err := waitWithRetry(t, compactable)
	if err != nil {
		t.Fatal(err)
	}

	// compact the revision from hour 2:06
	wreq := &pb.CompactionRequest{Revision: int64(1 + 2*n + 1)}
	if !reflect.DeepEqual(a[0].Params[0], wreq) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], wreq.Revision)
	}
}

func TestPeriodicSkipRevNotChange(t *testing.T) {
	retentionMinutes := 5
	retentionDuration := time.Duration(retentionMinutes) * time.Minute

	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStreamWithWaitTimout(0), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStreamWithWaitTimout(20 * time.Millisecond)}
	tb := newPeriodic(zaptest.NewLogger(t), fc, retentionDuration, 0, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// first compaction happens til 5 minutes elapsed
	for i := 0; i < initialIntervals-1; i++ {
		// every time set the same revision with 100
		rg.SetRev(int64(100))
		waitOneAction(t, rg)
		fc.Advance(tb.getRetryInterval())
	}

	// very first compaction
	a, err := waitWithRetry(t, compactable)
	if err != nil {
		t.Fatal(err)
	}

	// first compaction the compact revision will be 100+1
	expectedRevision := int64(100 + 1)
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}

	// compaction doesn't happens at every interval since revision not change
	for i := 0; i < 5; i++ {
		for j := 0; j < intervalsPerPeriod; j++ {
			rg.SetRev(int64(100))
			waitOneAction(t, rg)
			fc.Advance(tb.getRetryInterval())
		}

		_, err = compactable.Wait(1)
		if err == nil {
			t.Fatal(errors.New("should not compact since the revision not change"))
		}
	}

	// when revision changed, compaction is normally
	for i := 0; i < initialIntervals; i++ {
		waitOneAction(t, rg)
		fc.Advance(tb.getRetryInterval())
	}

	a, err = waitWithRetry(t, compactable)
	if err != nil {
		t.Fatal(err)
	}

	expectedRevision = int64(100 + 2)
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}
}

func waitOneAction(t *testing.T, r testutil.Recorder) {
	if actions, _ := r.Wait(1); len(actions) != 1 {
		t.Errorf("expect 1 action, got %v instead", len(actions))
	}
}

func waitWithRetry(t *testing.T, compactable *fakeCompactable) ([]testutil.Action, error) {
	t.Helper()

	var lastErr error
	var actions []testutil.Action

	expectedActions, maxRetries := 1, 5
	for retry := 0; retry < maxRetries; retry++ {
		actions, lastErr = compactable.Wait(expectedActions)
		if lastErr == nil || len(actions) >= expectedActions {
			return actions, nil
		}
		// Exponential backoff
		backoffTime := time.Duration(10*(1<<retry)) * time.Millisecond
		t.Logf("Retry %d: waiting %v before next attempt (last error: %v)", retry+1, backoffTime, lastErr)
		time.Sleep(backoffTime)
	}
	return nil, fmt.Errorf("after %d retries, last error: %w", maxRetries, lastErr)
}
