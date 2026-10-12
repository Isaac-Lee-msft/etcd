// Copyright 2026 The etcd Authors
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

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
)

// TestAutoCompactionInterval checks that --auto-compaction-interval sets how
// often auto compaction runs. Without it, revision mode would not compact for
// 5 minutes, so an old revision being compacted within seconds shows that the
// interval took effect.
func TestAutoCompactionInterval(t *testing.T) {
	tcs := []struct {
		name      string
		mode      string
		retention string
	}{
		{name: "revision", mode: embed.CompactorModeRevision, retention: "1"},
		{name: "periodic", mode: embed.CompactorModePeriodic, retention: "2s"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			e2e.BeforeTest(t)

			cfg := e2e.NewConfig(e2e.WithClusterSize(1))
			cfg.ServerConfig.AutoCompactionMode = tc.mode
			cfg.ServerConfig.AutoCompactionRetention = tc.retention
			cfg.ServerConfig.AutoCompactionInterval = "1s"
			epc, err := e2e.NewEtcdProcessCluster(t.Context(), t, e2e.WithConfig(cfg))
			require.NoError(t, err)
			defer func() {
				require.NoError(t, epc.Close())
			}()

			c := newClient(t, epc.EndpointsGRPC(), cfg.Client)
			first, err := c.Put(t.Context(), "foo", "0")
			require.NoError(t, err)
			for i := 1; i < 10; i++ {
				_, err = c.Put(t.Context(), "foo", fmt.Sprint(i))
				require.NoError(t, err)
			}

			require.Eventuallyf(t, func() bool {
				_, err := c.Get(t.Context(), "foo", clientv3.WithRev(first.Header.Revision))
				return err != nil && strings.Contains(err.Error(), rpctypes.ErrCompacted.Error())
			}, 15*time.Second, 100*time.Millisecond, "revision %d was not compacted", first.Header.Revision)
		})
	}
}
