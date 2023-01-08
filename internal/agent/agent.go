package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/yurakawa/proglog/internal/auth"
	"github.com/yurakawa/proglog/internal/discovery"
	"github.com/yurakawa/proglog/internal/log"
	"github.com/yurakawa/proglog/internal/server"
)

type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config: config,
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

// RPCアドレスにRaftとgRPCの両方の接続を受け付けるリスナーを作成し。そのリスナーでmuxを作成する。
// muxはリスナーからの接続を受け付け、設定されたルールに基づいてコネクションを識別する
func (a *Agent) setupMux() error {
	addr, err := net.ResolveTCPAddr("tcp", a.Config.BindAddr)
	if err != nil {
		return err
	}
	rpcAddr := fmt.Sprintf(
		"%s:%d",
		addr.IP.String(),
		a.Config.RPCPort,
	)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	// 一致したらRaft がコネクションを処理できるように、muxはraftリスナー用のコネクションを返します。
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		// 1バイトを読み込んで ストリームレイヤーで書き込んだ発信バイトと一致しているかチェックすることでRaftコネクションを識別する。
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)

	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	logConfig.Raft.BindAddr = rpcAddr
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	logConfig.Raft.CommitTimeout = 1000 * time.Millisecond

	// 前段で作成したlogConfigを使ってDis
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err

	// var err error
	// a.log, err = log.NewLog(
	// 	a.Config.DataDir,
	// 	log.Config{},
	// )
	// return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// RaftとgRPCの多重化のためgRPCサーバがmuxのリスナーを利用する様にする。

	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	// rpcAddr, err := a.Config.RPCAddr()
	// if err != nil {
	// 	return err
	// }
	// ln, err := net.Listen("tcp", rpcAddr)
	// if err != nil {
	// 	return err
	// }
	// go func() {
	// 	if err := a.server.Serve(ln); err != nil {
	// 		_ = a.Shutdown()
	// 	}
	// }()
	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

func (a *Agent) Shutdown() error {
	// シャットダウンするためのlockを取得する
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	// もしすでにシャットダウンしていたら何もしない
	if a.shutdown {
		return nil
	}
	// 一度だけshutdownするようにshutdownフラグを立てる
	a.shutdown = true
	// 各コンポーネントを閉じるメソッドをsliceにしている。
	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			// gracefulstopはerrorを返さないのでエラー型を返す無名関数にしている
			return nil
		},
		a.log.Close,
	}
	// shutdown funcsを順番に実行する
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
