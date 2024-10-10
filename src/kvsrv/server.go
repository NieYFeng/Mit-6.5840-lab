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
	mu    sync.Mutex
	kvMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvMap[key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	kv.kvMap[key] = args.Value
}

// appends arg to key's value and returns the old value,
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	reply.Value = kv.kvMap[key]
	kv.kvMap[key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	{
		kv.kvMap = make(map[string]string)
	}
	return kv
}
