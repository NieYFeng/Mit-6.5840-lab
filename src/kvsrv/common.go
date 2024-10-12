package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	RequestId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value   string
	ReplyId int64
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
