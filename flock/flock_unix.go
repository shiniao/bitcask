// +build !aix,!windows

package flock

import (
	"os"

	"golang.org/x/sys/unix"
)

// lock_sys 给文件加锁
func lock_sys(path string, nonBlocking bool) (_ *os.File, err error) {
	var fh *os.File
	// open a file with read and write
	fh, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	// defer close file head
	defer func() {
		if err != nil {
			fh.Close()
		}
	}()
	// 加上排它锁
	flag := unix.LOCK_EX
	// 如果是非阻塞版本，尝试对一个已经加锁的文件上锁会报错，而不是阻塞
	if nonBlocking {
		flag |= unix.LOCK_NB
	}
	// 给文件加锁
	err = unix.Flock(int(fh.Fd()), flag)
	if err != nil {
		return nil, err
	}

	if !sameInodes(fh, path) {
		return nil, ErrInodeChangedAtPath
	}

	return fh, nil
}

func rm_if_match(fh *os.File, path string) error {
	// Sanity check :
	// before running "rm", check that the file pointed at by the
	// filehandle has the same inode as the path in the filesystem
	//
	// If this sanity check doesn't pass, store a "ErrInodeChangedAtPath" error,
	// if the check passes, run os.Remove, and store the error if any.
	//
	// note : this sanity check is in no way atomic, but :
	//   - as long as only cooperative processes are involved, it will work as intended
	//   - it allows to avoid 99.9% the major pitfall case: "root user forcefully removed the lockfile"

	if !sameInodes(fh, path) {
		return ErrInodeChangedAtPath
	}

	return os.Remove(path)
}

func sameInodes(f *os.File, path string) bool {
	// get inode from opened file f:
	var fstat unix.Stat_t
	err := unix.Fstat(int(f.Fd()), &fstat)
	if err != nil {
		return false
	}
	fileIno := fstat.Ino

	// get inode for path on disk:
	var dstat unix.Stat_t
	err = unix.Stat(path, &dstat)
	if err != nil {
		return false
	}
	pathIno := dstat.Ino

	return pathIno == fileIno
}
