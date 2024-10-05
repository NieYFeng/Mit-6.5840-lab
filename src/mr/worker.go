package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		workerId := registerWorker()
		go sendHeartbeat(workerId)
		args := TaskRequest{WorkerState: Idle, WorkerId: workerId}
		reply := TaskResponse{}
		call("Coordinator.AllocateTasks", &args, &reply)
		// 如果任务类型为 idle，休眠一段时间，避免频繁请求
		if reply.TaskType == "idle" {
			time.Sleep(3 * time.Second)
			continue
		}
		if reply.TaskType == "map" {
			doMapWork(reply.FileName, mapf, reply.MapId, reply.NReduce)
			args = TaskRequest{WorkerState: MapFinished, WorkerId: workerId, FileName: reply.FileName}
			call("Coordinator.AllocateTasks", &args, &reply)
		} else {
			doReduceWork(reply.ReduceId, reducef, reply.MapCounter)
			args = TaskRequest{WorkerState: ReduceFinished, WorkerId: workerId, ReduceId: reply.ReduceId}
			call("Coordinator.AllocateTasks", &args, &reply)
		}
	}
}

func doMapWork(filename string, mapf func(string, string) []KeyValue, mapId int, n int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kvs := mapf(filename, string(content))
	intermediateFiles := make([]*os.File, n)
	encoders := make([]*json.Encoder, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("mr-%d-%d", mapId, i)
		intermediateFiles[i], err = os.Create(name)
		if err != nil {
			log.Fatalf("cannot create file %v", name)
		}
		encoders[i] = json.NewEncoder(intermediateFiles[i])
		defer intermediateFiles[i].Close()
	}
	for _, kv := range kvs {
		reduceId := ihash(kv.Key) % n
		err := encoders[reduceId].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv pair: %v", err)
		}
	}
}

func doReduceWork(reduceId int, reducef func(string, []string) string, n int) {
	intermediate := []KeyValue{}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, reduceId)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open file %v", name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatalf("Decode error: %v", err)
				}
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d.txt", reduceId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func sendHeartbeat(workerId int) {
	for {
		time.Sleep(3 * time.Second)
		args := HeartRequest{WorkerId: workerId}
		reply := HeartReply{}
		call("Coordinator.ReceiveHeartbeat", &args, &reply)
	}
}

func registerWorker() int {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		return reply.WorkerId
	}
	log.Fatal("Failed to register worker")
	return -1
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
