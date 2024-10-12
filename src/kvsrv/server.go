package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu              sync.Mutex
	kvMap           map[string]string
	pushAppendCache map[int64]PutAppendReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cachedReply, ok := kv.pushAppendCache[args.RequestId]; ok {
		*reply = cachedReply
		return
	}
	key := args.Key
	kv.kvMap[key] = args.Value
	kv.pushAppendCache[args.RequestId] = *reply
	if args.RequestId > 0 {
		delete(kv.pushAppendCache, args.RequestId-1)
	}
}

// appends arg to key's value and returns the old value,
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cachedReply, ok := kv.pushAppendCache[args.RequestId]; ok {
		*reply = cachedReply
		return
	}
	key := args.Key
	reply.Value = kv.kvMap[key]
	kv.kvMap[key] += args.Value
	reply.ReplyId = args.RequestId
	kv.pushAppendCache[args.RequestId] = *reply
	if args.RequestId > 0 {
		delete(kv.pushAppendCache, args.RequestId-1) // 清理上一个请求
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	{
		kv.kvMap = make(map[string]string)
		kv.pushAppendCache = make(map[int64]PutAppendReply)
	}
	return kv
}
