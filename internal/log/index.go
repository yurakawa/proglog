package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4                   // 4 bytes
	posWidth uint64 = 8                   // 8 bytes
	entWidth        = offWidth + posWidth // 12 bytes. オフセットが与えられたエントリの位置にジャンプするために使用する。
)

type index struct {
	file *os.File    // 永続化されたファイル
	mmap gommap.MMap // メモリマップされたファイル
	size uint64      // indexの現在のファイルサイズ。次のindexに追加されるエントリをどこに書き込むかを表す
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// 与えられたオフセットと位置をインデックスに追加する。

func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() { // エントリを書き込むための領域があるか確認する。
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off) // オフセットと位置をエンコードして、メモリにマップされたファイルに書き込む
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth) // 次の書き込みが行われる位置を進める。
	return nil
}

func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
