package server

import (
	"context"
	"time"

	api "github.com/yurakawa/proglog/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	// LogServerに定義されているエンドポイントについてUnimplementedエラーを返す
	api.UnimplementedLogServer
	*Config
}

// コンストラクタ関数
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		// 生でエラーを返してる
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		// 生でエラーを返してる
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStreamは双方向ストリーミングRPCを実装している。
// クライアントは複数のリクエストをサーバへストリーミングでき、サーバは各リクエストが成功した稼働をかクライアントに伝えられる。
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStreamはサーバ側のストリーミングRPCを実装しているので、クライアントはサーバにログ内のどのレコードを読み出すかを指示でき
// サーバはそのレコード移行のまだ書き込まれていたにレコードも含めてすべてのレコードをスクリーミングする。
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		// stream.Context().Done()を受けたとき
		case <-stream.Context().Done():
			return nil
		default:
			// TODO:[訳注]スピンループしそうなのでsleepを入れる
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				time.Sleep(time.Second)
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
