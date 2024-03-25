package kvraft

import (
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64      // 客户端唯一标识
	seqId    int64      // 该客户端单调递增的请求id
	leaderId int        // 缓存最近通讯的leader节点
	mu       sync.Mutex // Clerk的互斥锁
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	//todo 1. 构造Clerk对象，一个客户端对应多个服务端
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// todo 2.构造Get请求，并发送给leader节点，获取结果
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	DPrintf("Client[%d] Get starts, Key=%s ", ck.clientId, key)

	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()

	for { //todo 3.轮询重试，注意每次轮询不会改变Key、ClientId、SeqId
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK { // 命中
				return reply.Value
			} else if reply.Err == ErrNoKey { // 不存在
				return ""
			}
		}
		//todo 3.1如果没找到leader，重新选择leader
		ck.mu.Lock()
		leaderId = (leaderId + 1) % len(ck.servers)
		ck.leaderId = leaderId
		ck.mu.Unlock()

		time.Sleep(1 * time.Millisecond)
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	DPrintf("Client[%d] PutAppend, Key=%s Value=%s", ck.clientId, key, value)

	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()

	for { // 轮询重试，注意每次轮询不会改变Key、Value、Op、ClientId、SeqId
		reply := PutAppendReply{}
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK { // 成功
				break
			}
		}
		// 没找到leader，重新选择leader
		ck.mu.Lock()
		leaderId = (leaderId + 1) % len(ck.servers)
		ck.leaderId = leaderId
		ck.mu.Unlock()

		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
