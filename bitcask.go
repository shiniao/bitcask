package bitcask

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/flock"
	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/config"
	"github.com/prologic/bitcask/internal/data"
	"github.com/prologic/bitcask/internal/data/codec"
	"github.com/prologic/bitcask/internal/index"
	"github.com/prologic/bitcask/internal/metadata"
	"github.com/prologic/bitcask/scripts/migrations"
	log "github.com/sirupsen/logrus"
)

const (
	lockfile = "lock"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrKeyExpired is the error returned when a key is queried which has
	// already expired (due to ttl)
	ErrKeyExpired = errors.New("error: key expired")

	// ErrEmptyKey is the error returned for a value with an empty key.
	ErrEmptyKey = errors.New("error: empty key")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")

	ErrInvalidVersion = errors.New("error: invalid db version")

	// ErrMergeInProgress is the error returned if merge is called when already a merge
	// is in progress
	ErrMergeInProgress = errors.New("error: merge already in progress")
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu sync.RWMutex // 读写互斥锁

	*flock.Flock // 文件锁

	config    *config.Config        // 数据库配置
	options   []Option              // 数据库选项
	path      string                // 存储路径
	curr      data.Datafile         // 当前 datafile
	datafiles map[int]data.Datafile // 所有的 datafile
	trie      art.Tree              // trie 树
	indexer   index.Indexer         // 索引
	metadata  *metadata.MetaData    // 源数据
	isMerging bool                  // 是否正在合并
}

// Stats is a struct returned by Stats() on an open Bitcask instance
// 数据库状态
type Stats struct {
	Datafiles int
	Keys      int
	Size      int64
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *Bitcask) Stats() (stats Stats, err error) {
	if stats.Size, err = internal.DirSize(b.path); err != nil {
		return
	}

	b.mu.RLock()
	stats.Datafiles = len(b.datafiles)
	stats.Keys = b.trie.Size()
	b.mu.RUnlock()

	return
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only way to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	b.mu.RLock()
	defer func() {
		b.mu.RUnlock()
		b.Flock.Unlock()
	}()

	return b.close()
}

// 关闭
func (b *Bitcask) close() error {
	// 保存 index
	if err := b.saveIndex(); err != nil {
		return err
	}

	b.metadata.IndexUpToDate = true

	// 保存源数据
	if err := b.saveMetadata(); err != nil {
		return err
	}

	// 关闭所有 datafile
	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return err
		}
	}
	// 关闭当前 datafile
	return b.curr.Close()
}

// Sync flushes all buffers to disk ensuring all data is written
// 同步数据到磁盘
func (b *Bitcask) Sync() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// 保存源数据
	if err := b.saveMetadata(); err != nil {
		return err
	}
	// 同步当前 datafile
	return b.curr.Sync()
}

// Get fetches value for a key
// get value by key
func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// 获取 entry
	e, err := b.get(key)
	if err != nil {
		return nil, err
	}
	// 返回 value
	return e.Value, nil
}

// Has returns true if the key exists in the database, false otherwise.
// 查找是否有该 key
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	_, found := b.trie.Search(key)
	b.mu.RUnlock()
	return found
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key, value []byte, options ...PutOptions) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// key 和 value 超出大小，error
	if b.config.MaxKeySize > 0 && uint32(len(key)) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if b.config.MaxValueSize > 0 && uint64(len(value)) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}
	var feature Feature
	for _, opt := range options {
		if err := opt(&feature); err != nil {
			return err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// 写入 kv
	offset, n, err := b.put(key, value, feature)
	if err != nil {
		return err
	}
	// 如果配置了同步，同步到磁盘
	if b.config.Sync {
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	// in case of successful `put`, IndexUpToDate will be always be false
	b.metadata.IndexUpToDate = false
	// 如果trie 树中已经存在该 key，标记为回收
	if oldItem, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += oldItem.(internal.Item).Size
	}
	// 建立当前 kv item
	item := internal.Item{FileID: b.curr.FileID(), Offset: offset, Size: n}
	// 在 trie 树（index）中插入（或更新）
	b.trie.Insert(key, item)

	return nil
}

// Delete deletes the named key.
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.delete(key)
}

// delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) delete(key []byte) error {
	// 在数据库中将该 key 的 value 写为空
	_, _, err := b.put(key, []byte{}, Feature{})
	if err != nil {
		return err
	}
	// 在 trie 中找到该 key，待回收
	if item, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += item.(internal.Item).Size + codec.MetaInfoSize + int64(len(key))
	}
	// 在 trie 树中删除
	b.trie.Delete(key)

	return nil
}

// DeleteAll deletes all the keys. If an I/O error occurs the error is returned.
func (b *Bitcask) DeleteAll() (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		_, _, err = b.put(node.Key(), []byte{}, Feature{})
		if err != nil {
			return false
		}
		item, _ := b.trie.Search(node.Key())
		b.metadata.ReclaimableSpace += item.(internal.Item).Size + codec.MetaInfoSize + int64(len(node.Key()))
		return true
	})
	b.trie = art.New()

	return
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) (err error) {
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		// Skip the root node
		if len(node.Key()) == 0 {
			return true
		}

		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})
	return
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.trie.Size()
}

// Keys returns all keys in the database as a channel of keys
func (b *Bitcask) Keys() chan []byte {
	ch := make(chan []byte)
	go func() {
		b.mu.RLock()
		defer b.mu.RUnlock()

		for it := b.trie.Iterator(); it.HasNext(); {
			node, _ := it.Next()
			ch <- node.Key()
		}
		close(ch)
	}()

	return ch
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error returned.
func (b *Bitcask) Fold(f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})

	return
}

// get retrieves the value of the given key. If the key is not found or an/I/O
// error occurs a null byte slice is returned along with the error.
// get value by key
func (b *Bitcask) get(key []byte) (internal.Entry, error) {
	var df data.Datafile
	// 先从 trie 中查找 key 对应 entry 所在的文件位置
	value, found := b.trie.Search(key)
	if !found {
		return internal.Entry{}, ErrKeyNotFound
	}
	// item，item 是要查找键值对在磁盘中文件位置，偏移量和大小
	item := value.(internal.Item)
	// 如果是当前 datafile
	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		// 不是的话，通过 datafile 数组定位
		df = b.datafiles[item.FileID]
	}
	// 根据 offset 和 entry 的大小，从磁盘中读取 entry，并解码
	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return internal.Entry{}, err
	}
	// 如果数据已经过期了，删除该键值对
	if e.Expiry != nil && e.Expiry.Before(time.Now().UTC()) {
		_ = b.delete(key) // we don't care if it doesnt succeed
		return internal.Entry{}, ErrKeyExpired
	}
	// 校验和
	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return internal.Entry{}, ErrChecksumFailed
	}
	// 返回数据
	return e, nil
}

// put inserts a new (key, value). Both key and value are valid inputs.
// 存
func (b *Bitcask) put(key, value []byte, feature Feature) (int64, int64, error) {
	size := b.curr.Size()
	// 如果当前 datafile 超出
	if size >= int64(b.config.MaxDatafileSize) {
		// 关闭当前 datafile
		err := b.curr.Close()
		if err != nil {
			return -1, 0, err
		}

		id := b.curr.FileID()
		// 将当前 datafile 归档（只读）
		df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
		if err != nil {
			return -1, 0, err
		}
		// 归入 datafiles 集合
		b.datafiles[id] = df

		// id+1 后新建一个 datafile
		id = b.curr.FileID() + 1
		curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
		if err != nil {
			return -1, 0, err
		}
		// 将新建的 datafile 设为 curr
		b.curr = curr
		// 保存 index（当前 datafile 满了后，保存 index）
		err = b.saveIndex()
		if err != nil {
			return -1, 0, err
		}
	}
	// 当前 datafile 够用
	// 构建 entry，写入
	e := internal.NewEntry(key, value, feature.Expiry)
	return b.curr.Write(e)
}

// closeCurrentFile closes current datafile and makes it read only.
func (b *Bitcask) closeCurrentFile() error {
	err := b.curr.Close()
	if err != nil {
		return err
	}
	id := b.curr.FileID()
	df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}

	b.datafiles[id] = df
	return nil
}

// openNewWritableFile opens new datafile for writing data
func (b *Bitcask) openNewWritableFile() error {
	id := b.curr.FileID() + 1
	curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}
	b.curr = curr
	return nil
}

func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.reopen()
}

// reopen reloads a bitcask object with index and datafiles
// caller of this method should take care of locking
func (b *Bitcask) reopen() error {
	datafiles, lastID, err := loadDatafiles(b.path, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}
	t, err := loadIndex(b.path, b.indexer, b.config.MaxKeySize, datafiles, lastID, b.metadata.IndexUpToDate)
	if err != nil {
		return err
	}

	curr, err := data.NewDatafile(b.path, lastID, false, b.config.MaxKeySize, b.config.MaxValueSize, b.config.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}

	b.trie = t
	b.curr = curr
	b.datafiles = datafiles

	return nil
}

// Merge merges all datafiles in the database. Old keys are squashed
// and deleted keys removes. Duplicate key/value pairs are also removed.
// Call this function periodically to reclaim disk space.
// 合并 datafile，压缩旧key，删除不用的 key 和重复的 kv
func (b *Bitcask) Merge() error {
	b.mu.Lock()
	// 如果正在合并，unlock
	if b.isMerging {
		b.mu.Unlock()
		return ErrMergeInProgress
	}

	b.isMerging = true
	b.mu.Unlock()
	defer func() {
		b.isMerging = false
	}()

	// 添加读锁
	b.mu.RLock()
	// 关闭当前 datafile，并设置为只读
	err := b.closeCurrentFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}

	// 要合并的 datafile
	filesToMerge := make([]int, 0, len(b.datafiles))
	for k := range b.datafiles {
		filesToMerge = append(filesToMerge, k)
	}
	// 新打开一个可写 datafile
	err = b.openNewWritableFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	b.mu.RUnlock()

	// 对 datafile 排序，根据 fileID
	sort.Ints(filesToMerge)

	// Temporary merged database path
	// 创建一个临时合并文件夹
	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	// Create a merged database
	// 创建一个临时数据库
	mdb, err := Open(temp, withConfig(b.config))
	if err != nil {
		return err
	}

	// Rewrite all key/value pairs into merged database
	// Doing this automatically strips deleted keys and
	// old key/value pairs
	err = b.Fold(func(key []byte) error {
		item, _ := b.trie.Search(key)
		// if key was updated after start of merge operation, nothing to do
		if item.(internal.Item).FileID > filesToMerge[len(filesToMerge)-1] {
			return nil
		}
		e, err := b.get(key)
		if err != nil {
			return err
		}
		// prepare entry options
		var opts []PutOptions
		if e.Expiry != nil {
			opts = append(opts, WithExpiry(*(e.Expiry)))
		}

		if err := mdb.Put(key, e.Value, opts...); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	if err = mdb.Close(); err != nil {
		return err
	}
	// no reads and writes till we reopen
	b.mu.Lock()
	defer b.mu.Unlock()
	if err = b.close(); err != nil {
		return err
	}

	// Remove data files
	files, err := ioutil.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || file.Name() == lockfile {
			continue
		}
		ids, err := internal.ParseIds([]string{file.Name()})
		if err != nil {
			return err
		}
		// if datafile was created after start of merge, skip
		if len(ids) > 0 && ids[0] > filesToMerge[len(filesToMerge)-1] {
			continue
		}
		err = os.RemoveAll(path.Join(b.path, file.Name()))
		if err != nil {
			return err
		}
	}

	// Rename all merged data files
	files, err = ioutil.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err := os.Rename(
			path.Join([]string{mdb.path, file.Name()}...),
			path.Join([]string{b.path, file.Name()}...),
		)
		if err != nil {
			return err
		}
	}
	b.metadata.ReclaimableSpace = 0

	// And finally reopen the database
	return b.reopen()
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg  *config.Config
		err  error
		meta *metadata.MetaData
	)

	configPath := filepath.Join(path, "config.json")
	if internal.Exists(configPath) {
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, err
		}
	} else {
		cfg = newDefaultConfig()
	}

	if err := checkAndUpgrade(cfg, configPath); err != nil {
		return nil, err
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(path, cfg.DirFileModeBeforeUmask); err != nil {
		return nil, err
	}

	meta, err = loadMetadata(path)
	if err != nil {
		return nil, err
	}

	bitcask := &Bitcask{
		Flock:    flock.New(filepath.Join(path, lockfile)),
		config:   cfg,
		options:  options,
		path:     path,
		indexer:  index.NewIndexer(),
		metadata: meta,
	}

	locked, err := bitcask.Flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, ErrDatabaseLocked
	}

	if err := cfg.Save(configPath); err != nil {
		return nil, err
	}

	if cfg.AutoRecovery {
		if err := data.CheckAndRecover(path, cfg); err != nil {
			return nil, fmt.Errorf("recovering database: %s", err)
		}
	}
	if err := bitcask.Reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}

// checkAndUpgrade checks if DB upgrade is required
// if yes, then applies version upgrade and saves updated config
func checkAndUpgrade(cfg *config.Config, configPath string) error {
	if cfg.DBVersion == CurrentDBVersion {
		return nil
	}
	if cfg.DBVersion > CurrentDBVersion {
		return ErrInvalidVersion
	}
	// for v0 to v1 upgrade, we need to append 8 null bytes after each encoded entry in datafiles
	if cfg.DBVersion == uint32(0) && CurrentDBVersion == uint32(1) {
		log.Warn("upgrading db version, might take some time....")
		cfg.DBVersion = CurrentDBVersion
		return migrations.ApplyV0ToV1(filepath.Dir(configPath), cfg.MaxDatafileSize)
	}
	return nil
}

// Backup copies db directory to given path
// it creates path if it does not exist
func (b *Bitcask) Backup(path string) error {
	if !internal.Exists(path) {
		if err := os.MkdirAll(path, b.config.DirFileModeBeforeUmask); err != nil {
			return err
		}
	}
	return internal.Copy(b.path, path, []string{lockfile})
}

// saveIndex saves index currently in RAM to disk
// 保存 index 到磁盘
func (b *Bitcask) saveIndex() error {
	tempIdx := "temp_index"
	if err := b.indexer.Save(b.trie, filepath.Join(b.path, tempIdx)); err != nil {
		return err
	}
	return os.Rename(filepath.Join(b.path, tempIdx), filepath.Join(b.path, "index"))
}

// saveMetadata saves metadata into disk
func (b *Bitcask) saveMetadata() error {
	return b.metadata.Save(filepath.Join(b.path, "meta.json"), b.config.DirFileModeBeforeUmask)
}

// Reclaimable returns space that can be reclaimed
func (b *Bitcask) Reclaimable() int64 {
	return b.metadata.ReclaimableSpace
}

func loadDatafiles(path string, maxKeySize uint32, maxValueSize uint64, fileModeBeforeUmask os.FileMode) (datafiles map[int]data.Datafile, lastID int, err error) {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, 0, err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, 0, err
	}

	datafiles = make(map[int]data.Datafile, len(ids))
	for _, id := range ids {
		datafiles[id], err = data.NewDatafile(path, id, true, maxKeySize, maxValueSize, fileModeBeforeUmask)
		if err != nil {
			return
		}

	}
	if len(ids) > 0 {
		lastID = ids[len(ids)-1]
	}
	return
}

func getSortedDatafiles(datafiles map[int]data.Datafile) []data.Datafile {
	out := make([]data.Datafile, len(datafiles))
	idx := 0
	for _, df := range datafiles {
		out[idx] = df
		idx++
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FileID() < out[j].FileID()
	})
	return out
}

func loadIndex(path string, indexer index.Indexer, maxKeySize uint32, datafiles map[int]data.Datafile, lastID int, indexUpToDate bool) (art.Tree, error) {
	t, found, err := indexer.Load(filepath.Join(path, "index"), maxKeySize)
	if err != nil {
		return nil, err
	}
	if found && indexUpToDate {
		return t, nil
	}
	if found {
		if err := loadIndexFromDatafile(t, datafiles[lastID]); err != nil {
			return nil, err
		}
		return t, nil
	}
	sortedDatafiles := getSortedDatafiles(datafiles)
	for _, df := range sortedDatafiles {
		if err := loadIndexFromDatafile(t, df); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func loadIndexFromDatafile(t art.Tree, df data.Datafile) error {
	var offset int64
	for {
		e, n, err := df.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Tombstone value  (deleted key)
		if len(e.Value) == 0 {
			t.Delete(e.Key)
			offset += n
			continue
		}
		item := internal.Item{FileID: df.FileID(), Offset: offset, Size: n}
		t.Insert(e.Key, item)
		offset += n
	}
	return nil
}

func loadMetadata(path string) (*metadata.MetaData, error) {
	if !internal.Exists(filepath.Join(path, "meta.json")) {
		meta := new(metadata.MetaData)
		return meta, nil
	}
	return metadata.Load(filepath.Join(path, "meta.json"))
}
