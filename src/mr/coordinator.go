package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskInfo struct {
	TaskType string
	Value    string
}

type Coordinator struct {
	mapState         map[string]int //map任务状态[filename]状态信息
	reduceState      map[int]int    //reduce任务状态[id]状态信息
	mapCh            chan string
	reduceCh         chan int
	taskMap          int
	taskReduce       int
	files            []string //输入文件列表
	mapFinished      bool
	reduceFinished   bool
	workerHeartbeats map[int]time.Time // 记录每个 worker 的心跳时间
	workerTasks      map[int]TaskInfo
	workerCounter    int
	mutex            sync.Mutex //互斥锁
}

const (
	UnAllocated = iota
	Allocated
	Finished
)

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceFinished
}

func (c *Coordinator) AllocateTasks(args *TaskRequest, reply *TaskResponse) error {
	workerId := args.WorkerId
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.WorkerState == Idle {
		if len(c.mapCh) != 0 {
			filename := <-c.mapCh
			c.mapState[filename] = Allocated
			reply.TaskType = "map"
			reply.FileName = filename
			c.checkHeartBeat(workerId)
			return nil
		} else if len(c.reduceCh) != 0 && c.mapFinished == true {
			reduceId := <-c.reduceCh
			c.reduceState[reduceId] = Allocated
			reply.TaskType = "reduce"
			reply.ReduceId = reduceId
			c.checkHeartBeat(workerId)
			return nil
		}
	} else if args.WorkerState == MapFinished {
		c.mapState[args.FileName] = Finished
		if checkMapTask(c) {
			c.mapFinished = true
		}
	} else if args.WorkerState == ReduceFinished {
		c.reduceState[args.ReduceId] = Finished
		if checkReduceTask(c) {
			c.reduceFinished = true
		}
	}
	return nil
}

func (c *Coordinator) checkHeartBeat(workerId int) {
	for {
		time.Sleep(3 * time.Second) // 每3秒检查一次
		c.mutex.Lock()
		now := time.Now()
		if lastHeartbeat, ok := c.workerHeartbeats[workerId]; ok {
			if now.Sub(lastHeartbeat) > 10*time.Second { // 超过10秒无心跳
				log.Printf("Worker %s is assumed dead, reassigning tasks", workerId)
				delete(c.workerHeartbeats, workerId) // 移除 worker 心跳记录
				if taskInfo, ok := c.workerTasks[workerId]; ok {
					if taskInfo.TaskType == "map" {
						c.mapCh <- taskInfo.Value
						c.mapState[taskInfo.Value] = UnAllocated
					} else if taskInfo.TaskType == "reduce" {
						id, err := strconv.Atoi(taskInfo.Value)
						if err != nil {
							log.Printf("Failed to convert value to int: %v", err)
							continue
						}
						c.reduceCh <- id
						c.reduceState[id] = UnAllocated
					}
					delete(c.workerTasks, workerId) // 移除任务分配记录
				}
			}
		}
		c.mutex.Unlock()
	}
}

func (c *Coordinator) ReceiveHeartbeat(arg *HeartRequest, reply *HeartReply) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := time.Now()
	id := arg.WorkerId
	c.workerHeartbeats[id] = now
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

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.workerCounter++
	workerId := c.workerCounter
	reply.WorkerId = workerId
	c.workerHeartbeats[workerId] = time.Now() // 初始化心跳时间
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapState:       make(map[string]int),
		reduceState:    make(map[int]int),
		mapCh:          make(chan string),
		reduceCh:       make(chan int),
		taskMap:        len(files),
		taskReduce:     nReduce,
		files:          []string{},
		mapFinished:    false,
		reduceFinished: false,
		mutex:          sync.Mutex{},
	}
	for _, filename := range files {
		c.mapState[filename] = UnAllocated
	}
	for i := 0; i < nReduce; i++ {
		c.reduceState[i] = UnAllocated
	}
	c.server()
	return &c
}

func checkMapTask(c *Coordinator) bool {
	for _, state := range c.mapState {
		if state != Finished {
			return false
		}
	}
	return true
}

func checkReduceTask(c *Coordinator) bool {
	for _, state := range c.mapState {
		if state != Finished {
			return false
		}
	}
	return true
}
