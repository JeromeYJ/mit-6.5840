package lock

import (
	"fmt"
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	clientid string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = lockname
	lk.clientid = fmt.Sprintf("client-%d", time.Now().UnixNano())

	_, _, err := lk.ck.Get(lockname)
	if err == rpc.ErrNoKey {
		lk.ck.Put(lockname, "", 0)
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			log.Fatalf("there is no lock named %v", lk.lockname)
		} else if val == "" {
			lk.ck.Put(lk.lockname, lk.clientid, ver)

			// 验证是否真的获取到了锁（处理并发竞争）
			v, _, _ := lk.ck.Get(lk.lockname)
			if v == lk.clientid {
				return
			}
		} else if val == lk.clientid {
			return
		}

		// 锁被其他客户端持有，等待后重试
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	val, ver, err := lk.ck.Get(lk.lockname)

	if err != rpc.OK {
		log.Fatalf("there is no lock named %v", lk.lockname)
	} else if val == lk.clientid {
		lk.ck.Put(lk.lockname, "", ver)
	}
}
