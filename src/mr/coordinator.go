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
	mutex       sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, taskReply *TaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, mapTask := range c.mapTasks {
		if mapTask.taskState == READY {
			mapTask.timeStamp = time.Now()
			mapTask.taskState = BUSY
			taskReply.file = mapTask.file
			taskReply.taskType = MAP
			return nil
		}
	}
	for _, reduceTask := range c.reduceTasks {
		if reduceTask.taskState == READY {
			reduceTask.timeStamp = time.Now()
			reduceTask.taskState = BUSY
			taskReply.taskType = REDUCE
			return nil
		}
	}
	//TODO, implement suspension of workers when to tasks left
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
/* func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/
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
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		mapTasks:    make([]MapTask, len(files)),
		reduceTasks: make([]ReduceTask, nReduce),
	}
	for _, inputFile := range files {
		task := MapTask{
			file:      inputFile,
			taskType:  MAP,
			taskState: READY,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			taskType:  REDUCE,
			taskState: READY,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}
	c.server()
	return &c
}
