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
	MapId    int
}

type MapTask struct {
	MapId    int
	FileName string
}

type Coordinator struct {
	mapState         map[string]int // map任务状态[filename]状态信息
	reduceState      map[int]int    // reduce任务状态[id]状态信息
	mapCh            chan MapTask
	reduceCh         chan int
	taskReduce       int
	files            []string //输入文件列表
	mapFinished      bool
	reduceFinished   bool
	workerHeartbeats map[int]time.Time // 记录每个 worker 的心跳时间
	workerTasks      map[int]TaskInfo
	workerCounter    int
	mapCounter       int
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
	reply.NReduce = c.taskReduce
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.WorkerState == Idle {
		if len(c.mapCh) > 0 {
			task := <-c.mapCh
			filename := task.FileName
			c.mapState[filename] = Allocated
			reply.TaskType = "map"
			reply.FileName = filename
			reply.MapId = task.MapId
			c.workerTasks[workerId] = TaskInfo{"map", filename, task.MapId}
			c.checkHeartBeat(workerId)
			return nil
		} else if len(c.reduceCh) != 0 && c.mapFinished == true {
			reduceId := <-c.reduceCh
			c.reduceState[reduceId] = Allocated
			reply.TaskType = "reduce"
			reply.ReduceId = reduceId
			reply.MapCounter = c.mapCounter
			c.workerTasks[workerId] = TaskInfo{"reduce", strconv.Itoa(reduceId), -1}
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
	reply.TaskType = "idle"
	return nil
}

func (c *Coordinator) checkHeartBeat(workerId int) {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			c.mutex.Lock()
			now := time.Now()
			if lastHeartbeat, ok := c.workerHeartbeats[workerId]; ok {
				// 检查心跳超时
				if now.Sub(lastHeartbeat) > 10*time.Second {
					delete(c.workerHeartbeats, workerId)
					// 任务重新分配
					if taskInfo, ok := c.workerTasks[workerId]; ok {
						value, _ := strconv.Atoi(taskInfo.Value)
						if taskInfo.TaskType == "map" && c.mapState[taskInfo.Value] != Finished {
							c.mapCh <- MapTask{taskInfo.MapId, taskInfo.Value} // 将 map 任务重新放回队列
							c.mapState[taskInfo.Value] = UnAllocated
						} else if taskInfo.TaskType == "reduce" && c.reduceState[value] != Finished {
							id, err := strconv.Atoi(taskInfo.Value)
							if err != nil {
								log.Printf("Failed to convert value to int: %v", err)
								continue
							}
							c.reduceCh <- id // 将 reduce 任务重新放回队列
							c.reduceState[id] = UnAllocated
						}
						delete(c.workerTasks, workerId) // 移除该 worker 的任务记录
					}
				}
			}
			c.mutex.Unlock()
		}
	}()
}

func (c *Coordinator) ReceiveHeartbeat(arg *HeartRequest, reply *HeartReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := time.Now()
	id := arg.WorkerId
	c.workerHeartbeats[id] = now
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.workerCounter++
	workerId := c.workerCounter
	reply.WorkerId = workerId
	c.workerHeartbeats[workerId] = time.Now()
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapState:         make(map[string]int),
		reduceState:      make(map[int]int),
		mapCh:            make(chan MapTask, len(files)+5),
		reduceCh:         make(chan int, nReduce+5),
		workerHeartbeats: make(map[int]time.Time),
		taskReduce:       nReduce,
		workerTasks:      make(map[int]TaskInfo),
		files:            []string{},
		mapFinished:      false,
		reduceFinished:   false,
		mutex:            sync.Mutex{},
	}
	for i, filename := range files {
		mapId := i
		c.mapCounter++
		c.mapState[filename] = UnAllocated
		c.mapCh <- MapTask{FileName: filename, MapId: mapId}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceState[i] = UnAllocated
		c.reduceCh <- i
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
	for _, state := range c.reduceState {
		if state != Finished {
			return false
		}
	}
	return true
}
