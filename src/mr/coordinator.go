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
	nextWorkerId     int               // 下一个分配的 workerId
	mutex            sync.Mutex        //互斥锁
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.workerState == Idle {
		if len(c.mapCh) != 0 {
			filename := <-c.mapCh
			c.mapState[filename] = Allocated
			reply.TaskType = "map"
			reply.FileName = filename
			c.checkHeartBeat(reply.TaskType, filename)
			return nil
		} else if len(c.reduceCh) != 0 && c.mapFinished == true {
			reduceId := <-c.reduceCh
			c.reduceState[reduceId] = Allocated
			reply.TaskType = "reduce"
			reply.ReduceId = reduceId
			c.checkHeartBeat(reply.TaskType, strconv.Itoa(reduceId))
			return nil
		}
	} else if args.workerState == MapFinished {
		c.mapState[args.FileName] = Finished
		if checkMapTask(c) {
			c.mapFinished = true
		}
	} else if args.workerState == ReduceFinished {
		c.mapState[args.FileName] = Finished
		if checkReduceTask(c) {
			c.reduceFinished = true
		}
	}
	return nil
}

func (c *Coordinator) checkHeartBeat(key string, value string) {
	for {
		time.Sleep(5 * time.Second) // 每5秒检查一次
		c.mutex.Lock()
		now := time.Now()
		for workerId, lastHeartbeat := range c.workerHeartbeats {
			if now.Sub(lastHeartbeat) > 10*time.Second { // 超过10秒无心跳
				log.Printf("Worker %d is assumed dead, reassigning tasks", workerId)
				delete(c.workerHeartbeats, workerId) // 移除 worker
				if key == "map" {
					c.mapCh <- value
					c.mapState[value] = UnAllocated
				} else {
					id, err := strconv.Atoi(value)
					if err != nil {
						log.Printf("Failed to convert value to int: %v", err)
						continue
					}
					c.reduceCh <- id
					c.reduceState[id] = UnAllocated
				}
			}
		}
		c.mutex.Unlock()
	}
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
