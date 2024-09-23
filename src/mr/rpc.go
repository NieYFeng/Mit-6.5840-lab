package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	MapFinished = iota
	ReduceFinished
	Idle
)

type RegisterArgs struct {
	WorkerId int
}

type RegisterReply struct {
	WorkerId int
}

type HeartRequest struct {
	workerId int
}

type HeartReply struct {
}

type TaskRequest struct {
	reduceIdList []int
	workerState  int
	FileName     string
	ReduceId     int
	workerId     int
}

type TaskResponse struct {
	TaskType string
	FileName string
	ReduceId int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
