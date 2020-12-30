package flock

import (
	"errors"
	"os"
	"sync"
)
// 文件锁
type Flock struct {
	path string
	m    sync.Mutex
	fh   *os.File
}

var (
	ErrAlreadyLocked = errors.New("Double lock: already own the lock")
	ErrLockFailed    = errors.New("Could not acquire lock")
	ErrLockNotHeld   = errors.New("Could not unlock, lock is not held")

	ErrInodeChangedAtPath = errors.New("Inode changed at path")
)

// New returns a new instance of *Flock. The only parameter
// it takes is the path to the desired lockfile.
func New(path string) *Flock {
	return &Flock{path: path}
}

// Path returns the file path linked to this lock.
func (f *Flock) Path() string {
	return f.path
}

// Lock will acquire the lock. This function may block indefinitely if some other process holds the lock. For a non-blocking version, see Flock.TryLock().
// 加锁
func (f *Flock) Lock() error {
	f.m.Lock()
	defer f.m.Unlock()

	if f.fh != nil {
		return ErrAlreadyLocked
	}

	var fh *os.File
	// 加锁
	fh, err := lock_sys(f.path, false)
	// treat "ErrInodeChangedAtPath" as "some other process holds the lock, retry locking"
	for err == ErrInodeChangedAtPath {
		fh, err = lock_sys(f.path, false)
	}

	if err != nil {
		return err
	}
	if fh == nil {
		return ErrLockFailed
	}

	f.fh = fh
	return nil
}

// TryLock will try to acquire the lock, and returns immediately if the lock is already owned by another process.
// 尝试加锁，如果文件已经被锁住，返回
// 这是因为 unix.LOCK_NB
func (f *Flock) TryLock() (bool, error) {
	f.m.Lock()
	defer f.m.Unlock()

	if f.fh != nil {
		return false, ErrAlreadyLocked
	}

	fh, err := lock_sys(f.path, true)
	if err != nil {
		return false, ErrLockFailed
	}

	f.fh = fh
	return true, nil
}

// Unlock removes the lock file from disk and releases the lock.
// Whatever the result of `.Unlock()`, the caller must assume that it does not hold the lock anymore.
func (f *Flock) Unlock() error {
	f.m.Lock()
	defer f.m.Unlock()

	if f.fh == nil {
		return ErrLockNotHeld
	}

	err1 := rm_if_match(f.fh, f.path)
	err2 := f.fh.Close()

	if err1 != nil {
		return err1
	}
	return err2
}
