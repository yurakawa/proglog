package log_test

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/yurakawa/proglog/api/v1"
	"github.com/yurakawa/proglog/internal/log"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	// 3つのサーバから構成されるクラスタを設定する。
	nodeCount := 3
	ports := dynaport.Get(nodeCount) // 空いているportをとってくる
	for i := 0; i < nodeCount; i++ {
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		require.NoError(t, err)
		// for loopのなかでdeferしている。メモリ食うので推奨されていないけどcountが3でtestだからいいか
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)
		ln, err := net.Listen(
			"tcp",
			fmt.Sprintf("127.0.0.1:%d", ports[i]),
		)
		require.NoError(t, err)
		config := log.Config{}
		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		// タイムアウト設定を短くして、Raftが素早くリーダを選出できるようにする。
		config.Raft.HeartbeatTimeout = 100 * time.Millisecond
		config.Raft.ElectionTimeout = 100 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 100 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		// 一つ目のサーバは、クラスタをブートストラップしてリーダーになり、残りの二つのサーバをク
		// ラスタに追加しています。この後、リーダーは他のサーバをそのクラスタに参加させる必要があり
		// ます。
		if i == 0 {
			config.Raft.Bootstrap = true
		}
		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)
		if i != 0 {
			err = logs[0].Join(
				fmt.Sprintf("%d", i), ln.Addr().String(),
			)
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}
		logs = append(logs, l)
	}

	// リーダーのサーバにレコードを追加し、Raft がそのレコードをフォロワーに複製したことを確認
	// することで、レプリケーションをテストします。Raft のフォロワーは短い待ち時間の後に追加メッ
	// セージ（AppendRequestTypeのメッセージ）を適用するので、testifyのEventuallyメソッド
	// を使ってRaft の複製が終了するのに十分な時間を与えています。
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	for _, record := range records {
		// 0番目のサーバにappendを呼ぶ
		off, err := logs[0].Append(record)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				// この行いらんのでは
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}
	servers, err := logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	// リーダーがクラスタから離脱したサーバへのレプリケーションを停止し、既存の
	// サーバへのレプリケーションは継続することを確認しています
	err = logs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	servers, err = logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 2, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)

	time.Sleep(50 * time.Millisecond)
	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)
	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
