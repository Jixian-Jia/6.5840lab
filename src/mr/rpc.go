package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestTaskArgs struct {
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskReply struct {
	TaskType  TaskType
	FileNames []string
	NReduce   int
	TaskId    int // could be MapId or ReduceId, depending on tasktype.
}

type UpdateArgs struct {
	TaskId    int
	TaskType  TaskType
	FileNames []string
}

type UpdateReply struct {
	//do nothing
}
