package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/yurakawa/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// アクティブセグメントが最大サイズに達したときなど、あら棚セグメントを追加する必要がある時に呼ばれる。
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile( // storeファイルを開く
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, // 読み書き用にファイルを開くことを要求。ファイルが存在しなければos.O_CREATEフラグを渡してファイルを作成する。あれば追加モードでオープンする
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile( // indexファイルを開く
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, //  Appendはついていない
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// セグメントの次のオフセットを設定して、次に追加されるレコードのための準備をする。
	if off, _, err := s.index.Read(-1); err != nil { // -1は最後のエントリを表す。最後のエントリを読み出して、そのオフセットを取得する。
		// index空のパターン。セグメントに追加される次のレコードが最初のレコードとなり、そのオフセットはセグメントのベースオフセットになる
		s.nextOffset = baseOffset
	} else {
		// indexにエントリが存在するパターン。errorの中身はeEOF? 次に書き込まれるれコードのオフセットはセグメントの最後のオフセットを使う必要があり、
		// ベースオフセットと総体オフセットの和に1加算して得る。
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// セグメントにレコードを書き込み新たに追加されたレコードのオフセットを返す。
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// データをストアに追加
	// TODO: インデックスエントリ追加に失敗した場合storeで追加したレコードはゴミとしての残る。
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	// インデックスエントリを追加
	if err = s.index.Write(
		// インデックスのオフセットは、ベースオフセットからの相対的なものなので
		// セグメントのnextOffsetからbaseOffset(どちらも絶対オフセット)を引いて、セグメント内のエントリの相対オフセットを求める。
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	// netxOffsetに1を加算し、将来のAppendメソッドの呼び出しに備える
	s.nextOffset++
	return cur, nil
}

// 指定されたオフセットのレコードを返す。
func (s *segment) Read(off uint64) (*api.Record, error) {
	// 絶対オフセットを相対オフセットに変換しインデックスエントリの内容を取得する
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	// インデックスエントリから位置を取得するとセグメントはストア内のレコードの位置から適切な量のデータを読み出せる。
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// ストアまたはインデックスへの書き込みが一杯になったかどうかでセグメントが最大サイズに達したか判断する
// ログはこのメソッドを使って新たなセグメントを作成する必要があるかを知る。
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

// セグメントを閉じて、インデックスファイルとストアファイルを削除する
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
