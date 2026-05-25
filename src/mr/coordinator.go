package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	phase       Phase
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	nReduce     int
	files       []string
}

type TaskInfo struct {
	taskID    int
	file      string
	state     TaskState
	startTime time.Time
}
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Finish
)

type TaskState int

const (
	Scheduled TaskState = iota
	InProcess
	Done
)

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AllocateWork(args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	// allocate mapping or reducing work, depending on the phase
	// ask the worker to stop if all work are finished
	// put the worker on wait, if too much worker
	if c.phase == MapPhase {
		// find the first task to be scheduled
		var nextTask *TaskInfo
		var taskID int
		for i, task := range c.mapTasks {
			if task.state == Scheduled {
				nextTask = &task
				taskID = i
				break
			}
		}
		// if all works are Inprocess, tell the new worker to wait
		if nextTask == nil {
			reply.TaskType = WaitTask
		} else {
			reply.TaskType = MapTask
			reply.FileNames = []string{nextTask.file}
			reply.NReduce = c.nReduce
			reply.TaskId = taskID

			nextTask.startTime = time.Now()
			nextTask.state = InProcess
		}
	}
	// assume reduceTask has been initialized somewhere else
	if c.phase == ReducePhase {

	}

	return nil
}

func (c *Coordinator) UpdateWork(args *UpdateArgs, reply *UpdateReply) error {
	return nil //todo
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.phase = MapPhase
	c.files = files

	// initialize map tasks
	for i, f := range files {
		c.mapTasks = append(c.mapTasks, TaskInfo{taskID: i, file: f, state: Scheduled, startTime: time.Time{}})
	}

	// todo: the coor should periodically check on dead workers
	// the dead workers should also restart themselves
	c.server(sockname)
	return &c
}
