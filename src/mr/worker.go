package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	var taskID, nReduce int
	var taskType TaskType
	var fileNames []string

	for {
		task, err := RequestTask()
		if err == nil {
			taskID = task.TaskId
			taskType = task.TaskType
			fileNames = task.FileNames
			nReduce = task.NReduce
		} else {
			log.Fatal("error happened when request tasks") // exit
		}

		if taskType == MapTask {
			kva := mapTask(fileNames[0], mapf) //assume just one file for a worker

			// make buckets
			tmpfiles := []*os.File{}
			imfilenames := []string{}
			encoders := []*json.Encoder{}
			for i := 0; i < nReduce; i++ {
				imfilename := fmt.Sprintf("mr-%d-%d.json", taskID, i)
				// use temp files to write atomically
				tmp, err := os.CreateTemp("", imfilename+"-temp")
				if err != nil {
					panic(err)
				}
				imfilenames = append(imfilenames, imfilename)
				tmpfiles = append(tmpfiles, tmp)
				enc := json.NewEncoder(tmp)
				encoders = append(encoders, enc)
			}
			// partition the kv pairs

			for _, kv := range kva {
				bucket := ihash(kv.Key) % nReduce
				err := encoders[bucket].Encode(&kv)
				if err != nil {
					log.Fatal("error happened when saving intermediates") // exit
				}
			}

			for i, tmp := range tmpfiles {
				tmp.Close()
				os.Rename(tmp.Name(), imfilenames[i])
			}

			// notify the coordinator about the file locations
			err := updateState(taskID, taskType, imfilenames)
			if err != nil {
				log.Fatal("error happened when updating state")
			}
		}

		if taskType == WaitTask {
			fmt.Printf("task id:%d waiting", taskID)
			time.Sleep(time.Second) // sleep for 1 second
		}

		if taskType == ReduceTask {
			// fileNames now is a list of files to be reduced
			// open and read all the files into memory

			intermediate := []KeyValue{}
			for _, file := range fileNames {
				f, err := os.Open(file)
				if err != nil {
					panic(err)
				}

				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				f.Close()
			}
			// sort all pairs
			sort.Sort(ByKey(intermediate))
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			tmp, err := os.CreateTemp("", "mr-out-tmp*")
			if err != nil {
				log.Fatal("cannot create temp files")
			}

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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			tmp.Close()
			outputname := fmt.Sprintf("mr-out-%d", taskID)
			os.Rename(tmp.Name(), outputname)

			// notify the coordinator the finish of reduce
			err = updateState(taskID, taskType, []string{outputname})
			if err != nil {
				log.Fatal("error happened when updating state(reducer)")
			}
		}

		if taskType == ExitTask {
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func updateState(taskID int, taskType TaskType, fileNames []string) error {
	args := UpdateArgs{}
	args.FileNames = fileNames
	args.TaskId = taskID
	args.TaskType = taskType
	reply := UpdateReply{}
	ok := call("Coordinator.UpdateWork", &args, &reply)
	if ok {
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return fmt.Errorf("rpc failed")
	}

}

// performs the mapping and returns the list of kv pairs
func mapTask(fileName string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	return kva
}

func RequestTask() (RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.AllocateWork", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return RequestTaskReply{}, fmt.Errorf("rpc failed")
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
