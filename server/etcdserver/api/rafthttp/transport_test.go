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

package rafthttp

import (
	"errors"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/xiang90/probing"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/testing/protocmp"

	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/raft/v3/raftpb"
)

// TestTransportSend tests that transport can send messages using correct
// underlying peer, and drop local or unknown-target messages.
func TestTransportSend(t *testing.T) {
	peer1 := newFakePeer()
	peer2 := newFakePeer()
	tr := &Transport{
		ServerStats: stats.NewServerStats("", ""),
		peers:       map[types.ID]Peer{types.ID(1): peer1, types.ID(2): peer2},
	}
	wmsgsIgnored := []*raftpb.Message{
		// bad local message
		{Type: raftpb.MsgBeat.Enum()},
		// bad remote message
		{Type: raftpb.MsgProp.Enum(), To: new(uint64(3))},
	}
	wmsgsTo1 := []*raftpb.Message{
		// good message
		{Type: raftpb.MsgProp.Enum(), To: new(uint64(1))},
		{Type: raftpb.MsgApp.Enum(), To: new(uint64(1))},
	}
	wmsgsTo2 := []*raftpb.Message{
		// good message
		{Type: raftpb.MsgProp.Enum(), To: new(uint64(2))},
		{Type: raftpb.MsgApp.Enum(), To: new(uint64(2))},
	}
	tr.Send(wmsgsIgnored)
	tr.Send(wmsgsTo1)
	tr.Send(wmsgsTo2)

	if diff := cmp.Diff(wmsgsTo1, peer1.msgs, protocmp.Transform()); diff != "" {
		t.Errorf("unexpected peer1 msgs (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wmsgsTo2, peer2.msgs, protocmp.Transform()); diff != "" {
		t.Errorf("unexpected peer2 msgs (-want +got):\n%s", diff)
	}
}

func TestTransportCutMend(t *testing.T) {
	peer1 := newFakePeer()
	peer2 := newFakePeer()
	tr := &Transport{
		ServerStats: stats.NewServerStats("", ""),
		peers:       map[types.ID]Peer{types.ID(1): peer1, types.ID(2): peer2},
	}

	tr.CutPeer(types.ID(1))

	wmsgsTo := []*raftpb.Message{
		// good message
		{Type: raftpb.MsgProp.Enum(), To: new(uint64(1))},
		{Type: raftpb.MsgApp.Enum(), To: new(uint64(1))},
	}

	tr.Send(wmsgsTo)
	if len(peer1.msgs) > 0 {
		t.Fatalf("msgs expected to be ignored, got %+v", peer1.msgs)
	}

	tr.MendPeer(types.ID(1))

	tr.Send(wmsgsTo)
	if diff := cmp.Diff(wmsgsTo, peer1.msgs, protocmp.Transform()); diff != "" {
		t.Errorf("unexpected peer1 msgs after mend (-want +got):\n%s", diff)
	}
}

func TestTransportAdd(t *testing.T) {
	ls := stats.NewLeaderStats(zaptest.NewLogger(t), "")
	tr := &Transport{
		LeaderStats:    ls,
		streamRt:       &roundTripperRecorder{},
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}
	tr.AddPeer(1, []string{"http://localhost:2380"})

	if _, ok := ls.Followers["1"]; !ok {
		t.Errorf("FollowerStats[1] is nil, want exists")
	}
	s, ok := tr.peers[types.ID(1)]
	if !ok {
		tr.Stop()
		t.Fatalf("senders[1] is nil, want exists")
	}

	// duplicate AddPeer is ignored
	tr.AddPeer(1, []string{"http://localhost:2380"})
	ns := tr.peers[types.ID(1)]
	if s != ns {
		t.Errorf("sender = %v, want %v", ns, s)
	}

	tr.Stop()
}

func TestTransportRemove(t *testing.T) {
	tr := &Transport{
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		streamRt:       &roundTripperRecorder{},
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}
	tr.AddPeer(1, []string{"http://localhost:2380"})
	tr.RemovePeer(types.ID(1))
	defer tr.Stop()

	if _, ok := tr.peers[types.ID(1)]; ok {
		t.Fatalf("senders[1] exists, want removed")
	}
}

func TestTransportRemoveIsIdempotent(t *testing.T) {
	tr := &Transport{
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		streamRt:       &roundTripperRecorder{},
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}

	tr.AddPeer(1, []string{"http://localhost:2380"})
	tr.RemovePeer(types.ID(1))
	tr.RemovePeer(types.ID(1))
	defer tr.Stop()

	if _, ok := tr.peers[types.ID(1)]; ok {
		t.Fatalf("senders[1] exists, want removed")
	}
}

func TestTransportUpdate(t *testing.T) {
	peer := newFakePeer()
	tr := &Transport{
		peers:          map[types.ID]Peer{types.ID(1): peer},
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}
	u := "http://localhost:2380"
	tr.UpdatePeer(types.ID(1), []string{u})
	wurls := types.URLs(testutil.MustNewURLs(t, []string{"http://localhost:2380"}))
	if !reflect.DeepEqual(peer.peerURLs, wurls) {
		t.Errorf("urls = %+v, want %+v", peer.peerURLs, wurls)
	}
}

func TestTransportErrorc(t *testing.T) {
	errorc := make(chan error, 1)
	tr := &Transport{
		Raft:           &fakeRaft{},
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		ErrorC:         errorc,
		streamRt:       newRespRoundTripper(http.StatusForbidden, nil),
		pipelineRt:     newRespRoundTripper(http.StatusForbidden, nil),
		peers:          make(map[types.ID]Peer),
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}
	tr.AddPeer(1, []string{"http://localhost:2380"})
	defer tr.Stop()

	select {
	case <-errorc:
		t.Fatalf("received unexpected from errorc")
	case <-time.After(10 * time.Millisecond):
	}
	tr.peers[1].send(&raftpb.Message{})

	select {
	case <-errorc:
	case <-time.After(1 * time.Second):
		t.Fatalf("cannot receive error from errorc")
	}
}

// blockingWriteFlushCloser blocks every Write until released, like a
// connection to a peer that died without closing its TCP connection.
type blockingWriteFlushCloser struct {
	writing  chan struct{}
	released chan struct{}
	once     sync.Once
}

func newBlockingWriteFlushCloser() *blockingWriteFlushCloser {
	return &blockingWriteFlushCloser{writing: make(chan struct{}), released: make(chan struct{})}
}

func (w *blockingWriteFlushCloser) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.writing) })
	<-w.released
	return 0, errors.New("connection released")
}
func (w *blockingWriteFlushCloser) Flush()       {}
func (w *blockingWriteFlushCloser) Close() error { return nil }

// TestTransportRemovePeerWithBlockedWriterDoesNotBlockSend ensures that
// removing a peer whose stream writer is stuck on a half-open connection
// neither blocks the caller nor messages sent to other peers.
func TestTransportRemovePeerWithBlockedWriterDoesNotBlockSend(t *testing.T) {
	peer2 := newFakePeer()
	tr := &Transport{
		Logger:         zaptest.NewLogger(t),
		ID:             types.ID(100),
		Raft:           &fakeRaft{},
		ServerStats:    stats.NewServerStats("", ""),
		LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
		streamRt:       &roundTripperRecorder{},
		pipelineRt:     &roundTripperRecorder{},
		peers:          map[types.ID]Peer{types.ID(2): peer2},
		pipelineProber: probing.NewProber(nil),
		streamProber:   probing.NewProber(nil),
	}
	tr.AddPeer(types.ID(1), []string{"http://localhost:2380"})

	w := newBlockingWriteFlushCloser()
	defer func() {
		close(w.released)
		tr.Stop()
	}()
	tr.Get(types.ID(1)).attachOutgoingConn(&outgoingConn{
		t: streamTypeMessage, Writer: w, Flusher: w, Closer: w,
		localID: tr.ID, peerID: types.ID(1),
	})
	// The writer becomes working asynchronously; keep sending until it blocks.
	testutil.WaitSchedule()
	tr.Send([]*raftpb.Message{{Type: raftpb.MsgHeartbeat.Enum(), To: new(uint64(1))}})
	select {
	case <-w.writing:
	case <-time.After(time.Second):
		t.Fatal("stream writer did not start writing")
	}

	removed := make(chan struct{})
	go func() {
		tr.RemovePeer(types.ID(1))
		close(removed)
	}()
	select {
	case <-removed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RemovePeer blocked on a peer with a stuck stream writer")
	}

	sent := make(chan struct{})
	go func() {
		tr.Send([]*raftpb.Message{{Type: raftpb.MsgHeartbeat.Enum(), To: new(uint64(2))}})
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Send to a healthy peer blocked while another peer was being removed")
	}
	if tr.Get(types.ID(1)) != nil {
		t.Fatal("peer 1 still present after RemovePeer")
	}
}

// TestTransportStopWaitsForConcurrentRemovePeer runs RemovePeer and Stop
// concurrently: Stop must not return before every detached peer stopped, and
// the WaitGroup must not be misused (run with -race).
func TestTransportStopWaitsForConcurrentRemovePeer(t *testing.T) {
	for i := 0; i < 200; i++ {
		tr := &Transport{
			Logger:         zaptest.NewLogger(t),
			ID:             types.ID(100),
			Raft:           &fakeRaft{},
			ServerStats:    stats.NewServerStats("", ""),
			LeaderStats:    stats.NewLeaderStats(zaptest.NewLogger(t), ""),
			streamRt:       &roundTripperRecorder{},
			pipelineRt:     &roundTripperRecorder{},
			peers:          map[types.ID]Peer{},
			pipelineProber: probing.NewProber(nil),
			streamProber:   probing.NewProber(nil),
		}
		var fps []*fakePeer
		for id := types.ID(1); id <= 3; id++ {
			fp := newFakePeer()
			fps = append(fps, fp)
			tr.peers[id] = fp
		}
		var wg sync.WaitGroup
		for id := types.ID(1); id <= 3; id++ {
			wg.Add(1)
			go func() { defer wg.Done(); tr.RemovePeer(id) }()
		}
		tr.Stop()
		wg.Wait()
		for j, fp := range fps {
			if !fp.stopped.Load() {
				t.Fatalf("iteration %d: peer %d not stopped after Stop and RemovePeer returned", i, j+1)
			}
		}
	}
}
