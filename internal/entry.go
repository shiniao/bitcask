package internal

import (
	"hash/crc32"
	"time"
)

// Entry represents a key/value in the database
// 数据库中（datafile）的键值对
type Entry struct {
	Checksum uint32     // 校验和
	Key      []byte     // 键
	Offset   int64      // 偏移
	Value    []byte     // 值
	Expiry   *time.Time // 有效期
}

// NewEntry creates a new `Entry` with the given `key` and `value`
func NewEntry(key, value []byte, expiry *time.Time) Entry {
	checksum := crc32.ChecksumIEEE(value)

	return Entry{
		Checksum: checksum,
		Key:      key,
		Value:    value,
		Expiry:   expiry,
	}
}
