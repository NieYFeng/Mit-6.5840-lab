package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := registerWorker()
	go sendHeartbeat(workerId)
	args := TaskRequest{WorkerState: Idle, WorkerId: workerId}
	reply := TaskResponse{}
	call("Coordinator.AllocateTasks", &args, &reply)
	if reply.TaskType == "map" {
		doMapWork(reply.FileName)
		args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName}
		call("Coordinator.AllocateTasks", &args, &reply)
	} else {
		args = TaskRequest{WorkerState: ReduceFinished, WorkerId: workerId, ReduceId: reply.ReduceId}
		call("Coordinator.AllocateTasks", &args, &reply)
		doReduceWork(reply.ReduceId)
	}
}

func doMapWork(filename string) {

}

func sendHeartbeat(workerId int) {
	for {
		time.Sleep(3 * time.Second) // 每 3 秒发送一次心跳
		args := HeartRequest{WorkerId: workerId}
		reply := HeartReply{}
		call("Coordinator.ReceiveHeartbeat", &args, &reply)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
