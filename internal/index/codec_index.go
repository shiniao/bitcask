package index

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
)

var (
	errTruncatedKeySize = errors.New("key size is truncated")
	errTruncatedKeyData = errors.New("key data is truncated")
	errTruncatedData    = errors.New("data is truncated")
	errKeySizeTooLarge  = errors.New("key size too large")
)

// key:value 大小
const (
	int32Size  = 4
	int64Size  = 8
	fileIDSize = int32Size
	offsetSize = int64Size
	sizeSize   = int64Size
)

// 每次从 fd 读取固定大小的字节
func readKeyBytes(r io.Reader, maxKeySize uint32) ([]byte, error) {
	s := make([]byte, int32Size)
	// 读取 int32 大小的字节段
	_, err := io.ReadFull(r, s)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.Wrap(errTruncatedKeySize, err.Error())
	}
	// 将字节序列转换为 uint32
	size := binary.BigEndian.Uint32(s)
	// 超出大小，error
	if maxKeySize > 0 && size > uint32(maxKeySize) {
		return nil, errKeySizeTooLarge
	}

	b := make([]byte, size)
	// 读取 size 大小字节段，也就是 key 部分
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, errors.Wrap(errTruncatedKeyData, err.Error())
	}
	return b, nil
}

// 写入固定大小的字节
func writeBytes(b []byte, w io.Writer) error {
	s := make([]byte, int32Size)
	// 转换 key 大小到字节序列
	binary.BigEndian.PutUint32(s, uint32(len(b)))
	// 写入 int32 大小的字节，也就是 key 大小
	_, err := w.Write(s)
	if err != nil {
		return err
	}
	// 写入 key 值（b）
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

// 读 item
func readItem(r io.Reader) (internal.Item, error) {
	buf := make([]byte, fileIDSize+offsetSize+sizeSize)
	// 读字节段（fileID+offset+size）
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return internal.Item{}, errors.Wrap(errTruncatedData, err.Error())
	}
	// 字节段转换为 int，同时赋值给 item
	return internal.Item{
		FileID: int(binary.BigEndian.Uint32(buf[:fileIDSize])),
		Offset: int64(binary.BigEndian.Uint64(buf[fileIDSize:(fileIDSize + offsetSize)])),
		Size:   int64(binary.BigEndian.Uint64(buf[(fileIDSize + offsetSize):])),
	}, nil
}

// 写 item
func writeItem(item internal.Item, w io.Writer) error {
	buf := make([]byte, fileIDSize+offsetSize+sizeSize)
	// 转换 FileID 到字节序列
	binary.BigEndian.PutUint32(buf[:fileIDSize], uint32(item.FileID))
	// 转换 Offset 到字节序列
	binary.BigEndian.PutUint64(buf[fileIDSize:(fileIDSize+offsetSize)], uint64(item.Offset))
	// 转换 key+value 大小到字节序列
	binary.BigEndian.PutUint64(buf[(fileIDSize+offsetSize):], uint64(item.Size))
	// 写入 buf 到磁盘
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

// ReadIndex reads a persisted from a io.Reader into a Tree
// 从 fd 读取 index
func readIndex(r io.Reader, t art.Tree, maxKeySize uint32) error {
	// 循环从 r 中读取固定大小的字节数
	for {
		// 从 fd 读取固定大小（key 大小）字节
		key, err := readKeyBytes(r, maxKeySize)
		if err != nil {
			// 文件读完了，退出
			if err == io.EOF {
				break
			}
			return err
		}
		// 读完 key，读 item
		item, err := readItem(r)
		if err != nil {
			return err
		}
		// 将 key 和 item 存入 trie 树
		t.Insert(key, item)
	}

	return nil
}

// 写入 index
func writeIndex(t art.Tree, w io.Writer) (err error) {
	// 循环写入每一个 node
	t.ForEach(func(node art.Node) bool {
		// 写入 key
		err = writeBytes(node.Key(), w)
		if err != nil {
			return false
		}

		item := node.Value().(internal.Item)
		// 写入 item
		err := writeItem(item, w)
		return err == nil
	})
	return
}

// IsIndexCorruption returns a boolean indicating whether the error
// is known to report a corruption data issue
func IsIndexCorruption(err error) bool {
	cause := errors.Cause(err)
	switch cause {
	case errKeySizeTooLarge, errTruncatedData, errTruncatedKeyData, errTruncatedKeySize:
		return true
	}
	return false
}
