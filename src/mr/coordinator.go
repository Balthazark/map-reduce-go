package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	file      string
	id        int
	timestamp time.Time
	taskState TaskState
}

type Coordinator struct {
	mutex                sync.Mutex
	mapTasks             []Task
	reduceTasks          []Task
	mapTasksRemaining    int
	reduceTasksRemaining int
	nReduce              int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, taskReply *TaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapTasksRemaining > 0 {
		for _, task := range c.mapTasks {
			if task.taskState == READY {
				task.timestamp = time.Now()
				task.taskState = BUSY
				taskReply.file = task.file
				taskReply.id = task.id
				taskReply.nReduce = c.nReduce
				taskReply.taskType = MAP
				return nil
			}
		}
	}

	if c.reduceTasksRemaining > 0 {
		for _, task := range c.reduceTasks {
			if task.taskState == READY {
				task.timestamp = time.Now()
				task.taskState = BUSY
				taskReply.id = task.id
				taskReply.taskType = REDUCE
				return nil

			}
		}
	}
	return errors.New("all tasks completed")
}

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
		mapTasks:             make([]Task, len(files)),
		reduceTasks:          make([]Task, nReduce),
		mutex:                sync.Mutex{},
		mapTasksRemaining:    len(files),
		reduceTasksRemaining: nReduce,
		nReduce:              nReduce,
	}
	for i, inputFile := range files {
		task := Task{
			file:      inputFile,
			id:        i,
			taskState: READY,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := Task{
			id:        i,
			taskState: READY,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	//Implement time out tasks

	c.server()
	return &c
}
