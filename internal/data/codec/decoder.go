package codec

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal"
)

var (
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errCantDecodeOnNilEntry  = errors.New("can't decode on nil entry")
	errTruncatedData         = errors.New("data is truncated")
)

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(r io.Reader, maxKeySize uint32, maxValueSize uint64) *Decoder {
	return &Decoder{
		r:            r,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}
}

// Decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type Decoder struct {
	r            io.Reader
	maxKeySize   uint32
	maxValueSize uint64
}

// Decode decodes the next Entry from the current stream
// 读取文件，从 fd 流中一个 entry 一个 entry 的读取，并解码
// 一个键值对 entry 在磁盘中的存放格式：
// keySize:ValueSize:Key:Value:Checksum:ttl
func (d *Decoder) Decode(v *internal.Entry) (int64, error) {
	if v == nil {
		return 0, errCantDecodeOnNilEntry
	}

	prefixBuf := make([]byte, keySize+valueSize)

	// 从 fd 中读取固定数量的数据到 prefixBuf
	// 读取 key 和 value 所占的字节大小
	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}
	// 根据 key 和 value 的大小找到 key 和 value 的位置
	actualKeySize, actualValueSize, err := getKeyValueSizes(prefixBuf, d.maxKeySize, d.maxValueSize)
	if err != nil {
		return 0, err
	}

	// 读取加上校验和等数据的字节段
	buf := make([]byte, uint64(actualKeySize)+actualValueSize+checksumSize+ttlSize)
	if _, err = io.ReadFull(d.r, buf); err != nil {
		return 0, errTruncatedData
	}
	// 获取每一部分（key、value、checksum等）所在的位置
	decodeWithoutPrefix(buf, actualKeySize, v)

	// 返回所有数据的大小
	return int64(keySize + valueSize + uint64(actualKeySize) + actualValueSize + checksumSize + ttlSize), nil
}

// DecodeEntry decodes a serialized entry
func DecodeEntry(b []byte, e *internal.Entry, maxKeySize uint32, maxValueSize uint64) error {
	valueOffset, _, err := getKeyValueSizes(b, maxKeySize, maxValueSize)
	if err != nil {
		return errors.Wrap(err, "key/value sizes are invalid")
	}

	decodeWithoutPrefix(b[keySize+valueSize:], valueOffset, e)

	return nil
}

// 获取 key 和 value 的大小
func getKeyValueSizes(buf []byte, maxKeySize uint32, maxValueSize uint64) (uint32, uint64, error) {
	// 实际的 key 字节大小
	actualKeySize := binary.BigEndian.Uint32(buf[:keySize])
	// 实际的 value 字节大小
	actualValueSize := binary.BigEndian.Uint64(buf[keySize:])

	// 如果 key 和 value 的大小超出最大限制，error
	if (maxKeySize > 0 && actualKeySize > maxKeySize) || (maxValueSize > 0 && actualValueSize > maxValueSize) || actualKeySize == 0 {

		return 0, 0, errInvalidKeyOrValueSize
	}

	return actualKeySize, actualValueSize, nil
}

// 获取每一部分数据所在的位置
func decodeWithoutPrefix(buf []byte, valueOffset uint32, v *internal.Entry) {
	v.Key = buf[:valueOffset]
	v.Value = buf[valueOffset : len(buf)-checksumSize-ttlSize]
	v.Checksum = binary.BigEndian.Uint32(buf[len(buf)-checksumSize-ttlSize : len(buf)-ttlSize])
	v.Expiry = getKeyExpiry(buf)
}

func getKeyExpiry(buf []byte) *time.Time {
	expiry := binary.BigEndian.Uint64(buf[len(buf)-ttlSize:])
	if expiry == uint64(0) {
		return nil
	}
	t := time.Unix(int64(expiry), 0).UTC()
	return &t
}

// IsCorruptedData indicates if the error correspondes to possible data corruption
func IsCorruptedData(err error) bool {
	switch err {
	case errCantDecodeOnNilEntry, errInvalidKeyOrValueSize, errTruncatedData:
		return true
	default:
		return false
	}
}
