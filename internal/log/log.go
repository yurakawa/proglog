package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/yurakawa/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	// 書き込みを追加するアクティブセグメントへのポインタ
	activeSegment *segment
	// セグメントの集まり
	segments []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	// デフォルト値の設定
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	// Logのインスタンスを作成して、 出力dirとコンフィグを設定する
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	// セグメントの一覧を取得する。
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	// ファイル名からベースオフセットの値を取得。
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// 取得したベースオフセットの値をソートしている。セグメントのスライスを古い順に並べたいから。
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	// 既存のセグメントがない場合、下の処理はスキップされる。
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsetsはインデックスとストアの２つの重複を含んでいるので重複しているものをスキップする
		i++
	}
	// 既存のセグメントがない場合、newSegmentヘルパーメソッドを使って渡されたベースオフセット(InitialOffset)で最初のセグメントを作成する。
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// ログにレコードを追加する。
// TODO: ログ全体でなくセグメントごとにロックを獲得する
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	// アクティブセグメントが最大サイズ以上のときは、新しいセグメントを作成する。
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	// アクティブセグメントにレコードを追加する。
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, err
}

// 指定されたオフセットに保存されているレコードを読み出す。
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	// セグメントの一覧をループして、指定されたオフセット(レコード?)が含まれているセグメントを探す。
	for _, segment := range l.segments {
		// セグメントは古い順に並んでおり、セグメントのベースオフセットはセグメント内の最小のオフセットなので、
		//ベースセットが探しているオフセット以下であり、
		//かつnextOffsetが探しているオフセットより大きい、最初のオフセットを探している。
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	// || s.nextOffset <= offはいらなそう
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	// レコードを含むセグメントセグメントを見つけたら、そのセグメントのインデックスからインデックスエントリを取得して
	// ストアファイルからデータを読み出して、そのデータを呼び出し元に返す。
	return s.Read(off)
}

// セグメントをすべてクローズする
func (l *Log) Close() error {
	// read/writeロックを取得する
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// ログをクローズして、そのデータをすべて索状sる
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// ログを削除して、置き換える新たなログを作成する。
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// ログに保存されているオフセット範囲を教えてくれる
// レプリケーションを行う連携型クラスタのサポートに取り組む際に、どのノードが最も古いデータと最新のデータを持っているか、どのノードが遅れていてレプリケーションを行う必要があるか知るために、
// ログに保存されているオフセット範囲の情報が必要になる。
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// ディスク容量の節約のため、定期的にTruncateを呼び出して、それまでに処理したデータで不要になった古いセグメントを削除する
func (l *Log) Truncate(lowest uint64) error {
	// ロックをして処理を排他にしている。
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	// 最大オフセットがlowestより小さいセグメントをすべて削除する。
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// ログ全体を読み込むためのio.Readerを返す。
// 合意形成の連携を実装し、スナップショットをサポートし、ログの復旧をサポートする必要がある場合、
// この機能が必要になる。
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		// Readerメソッドはio.MultiReaderを使ってセグメントのストアを連結している。
		// セグメントのストアはoriginReader型で保持されている。
		// 理由は
		// - io.Readerインタフェースを満たし、それをio.MultiReader呼び出しに渡すためです。
		// - ストアの最初から読み込みを開始し、そのファイル全体を読み込むことを保証するため
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)

}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.store.ReadAt(p, o.off)
	// 呼んだ文だけオフセットをずらす
	o.off += int64(n)
	return n, err
}

// 新しいセグメントを作成して、ログのセグメントのスライスに追加する
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	// 新たなセグメントをアクティブなセグメントとして設定しているため
	// その後のAppendメソッドの呼び出しは新たなセグメントに書き込む
	l.activeSegment = s
	return nil
}
