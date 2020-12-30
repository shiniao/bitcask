package index

import (
	"os"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
)

// trie 树中存放的是：
// key2:item2  key2:item2

// Indexer is an interface for loading and saving the index (an Adaptive Radix Tree)
// 索引：trie 树，索引就是 trie 树。
type Indexer interface {
	Load(path string, maxkeySize uint32) (art.Tree, bool, error)
	Save(t art.Tree, path string) error
}

// NewIndexer returns an instance of the default `Indexer` implementation
// which persists the index (an Adaptive Radix Tree) as a binary blob on file
func NewIndexer() Indexer {
	return &indexer{}
}

type indexer struct{}

// 从磁盘 path 加载 index
func (i *indexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	// 新建 trie 索引树
	t := art.New()

	if !internal.Exists(path) {
		return t, false, nil
	}
	// 打开路径
	f, err := os.Open(path)
	if err != nil {
		return t, true, err
	}
	defer f.Close()
	// 读取 fd 中的内容到 trie
	if err := readIndex(f, t, maxKeySize); err != nil {
		return t, true, err
	}
	return t, true, nil
}
// 将 index 写入磁盘
func (i *indexer) Save(t art.Tree, path string) error {
	// 打开文件
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	// 写 index 到 fd（此时只是在缓存中）
	if err := writeIndex(t, f); err != nil {
		return err
	}
	// 同步到磁盘
	if err := f.Sync(); err != nil {
		return err
	}

	return f.Close()
}
