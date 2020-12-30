package data

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/data/codec"
	"golang.org/x/exp/mmap"
)

const (
	// 默认数据文件名
	defaultDatafileFilename = "%09d.data"
)

var (
	errReadonly  = errors.New("error: read only datafile")
	errReadError = errors.New("error: read error")
)

// Datafile is an interface that represents a readable and writeable datafile
type Datafile interface {
	FileID() int
	Name() string
	Close() error
	Sync() error
	Size() int64
	Read() (internal.Entry, int64, error)
	ReadAt(index, size int64) (internal.Entry, error)
	Write(internal.Entry) (int64, int64, error)
}

type datafile struct {
	sync.RWMutex // 读写锁

	id           int            // 标识
	r            *os.File       // read文件描述符
	ra           *mmap.ReaderAt // 读取内存映射文件
	w            *os.File       // write文件描述符
	offset       int64          // 偏移量
	dec          *codec.Decoder // decode
	enc          *codec.Encoder // encode
	maxKeySize   uint32         // 最大 key 大小
	maxValueSize uint64         // 最大 value 大小
}

// NewDatafile opens an existing datafile
func NewDatafile(path string, id int, readonly bool, maxKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))

	// 文件可写，打开写 fd
	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, fileMode)
		if err != nil {
			return nil, err
		}
	}
	// 读文件 fd
	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}
	stat, err := r.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error calling Stat()")
	}
	// mmap 打开文件（打开内存映射的文件）
	ra, err = mmap.Open(fn)
	if err != nil {
		return nil, err
	}
	// 偏移量为文件大小
	offset := stat.Size()
	// 读的文件，解码
	dec := codec.NewDecoder(r, maxKeySize, maxValueSize)
	// 写的文件，编码
	enc := codec.NewEncoder(w)

	return &datafile{
		id:           id,
		r:            r,
		ra:           ra,
		w:            w,
		offset:       offset,
		dec:          dec,
		enc:          enc,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}, nil
}

func (df *datafile) FileID() int {
	return df.id
}

func (df *datafile) Name() string {
	return df.r.Name()
}

func (df *datafile) Close() error {
	defer func() {
		df.ra.Close()
		df.r.Close()
	}()

	// Readonly datafile -- Nothing further to close on the write side
	if df.w == nil {
		return nil
	}
	// 关闭之前先刷入磁盘
	err := df.Sync()
	if err != nil {
		return err
	}
	return df.w.Close()
}

// 刷入 write fd 到磁盘
func (df *datafile) Sync() error {
	if df.w == nil {
		return nil
	}
	return df.w.Sync()
}

func (df *datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

// Read reads the next entry from the datafile
// 读数据文件
func (df *datafile) Read() (e internal.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()
	// 从 datafile 读取 entry，解码
	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

// ReadAt the entry located at index offset with expected serialized size
// 读固定大小 entry
func (df *datafile) ReadAt(index, size int64) (e internal.Entry, err error) {
	var n int

	b := make([]byte, size)
	// 如果文件只读
	if df.w == nil {
		n, err = df.ra.ReadAt(b, index)
	} else {
		n, err = df.r.ReadAt(b, index)
	}
	if err != nil {
		return
	}
	if int64(n) != size {
		err = errReadError
		return
	}

	codec.DecodeEntry(b, &e, df.maxKeySize, df.maxValueSize)

	return
}

// 写入 entry 到 datafile
func (df *datafile) Write(e internal.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, errReadonly
	}

	df.Lock()
	defer df.Unlock()

	e.Offset = df.offset
	// 编码
	n, err := df.enc.Encode(e)
	if err != nil {
		return -1, 0, err
	}
	// 偏移量 +
	df.offset += n

	return e.Offset, n, nil
}
