package mr

import (
	"errors"
	"fmt"
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

    if c.mapTasksRemaining > 0 {
        for i := range c.mapTasks {
            task := &c.mapTasks[i] 
            if task.taskState == READY {
                task.timestamp = time.Now()
                task.taskState = BUSY
                taskReply.File = task.file
                taskReply.Id = task.id
                taskReply.NReduce = c.nReduce
                taskReply.TaskType = MAP
				c.mutex.Unlock()
                return nil
            }
        }
		c.mutex.Unlock()
		return nil
    }

    if c.reduceTasksRemaining > 0 {
        for i := range c.reduceTasks {
            task := &c.reduceTasks[i]
            if task.taskState == READY {
                task.timestamp = time.Now()
                task.taskState = BUSY
                taskReply.Id = task.id
                taskReply.TaskType = REDUCE
				c.mutex.Unlock()
                return nil
            }
        }
    }
    c.mutex.Unlock()
    return errors.New("all tasks completed")
}


func (c *Coordinator) TaskComplete(args *TaskDoneArgs, reply *TaskArgs) error {
	c.mutex.Lock()

	if args.TaskType == MAP && c.mapTasks[args.TaskId].taskState != COMPLETED {
		c.mapTasks[args.TaskId].taskState = COMPLETED
		c.mapTasksRemaining--
	} else if args.TaskType == REDUCE && c.reduceTasks[args.TaskId].taskState != COMPLETED {
		c.reduceTasks[args.TaskId].taskState = COMPLETED
		c.reduceTasksRemaining--
	}
	c.mutex.Unlock()
	return nil
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
	c.mutex.Lock()
	if c.mapTasksRemaining == 0 && c.reduceTasksRemaining == 0 {
		fmt.Print("DONE")
		c.mutex.Unlock()
		return true
	} else {
		c.mutex.Unlock()
		return false
	}
}

// Handle timeouts for workers
func (c *Coordinator) ReAssignTimeoutTasks() {
	for {
		c.mutex.Lock()
		if c.mapTasksRemaining > 0 {

			for i := range c.mapTasks {
				task := &c.mapTasks[i]
				if task.taskState == BUSY && time.Since(task.timestamp).Seconds() > 10 {
					task.taskState = READY
				}
			}
		} else if c.reduceTasksRemaining > 0 {
			for i := range c.reduceTasks {
				task := &c.reduceTasks[i]
				if task.taskState == BUSY && time.Since(task.timestamp).Seconds() > 10 {
					task.taskState = READY
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
	// Your code here.
	c := Coordinator{
		mapTasks:             make([]Task, len(files)), // Initialize the slice with the correct length
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
		c.mapTasks[i] = task

	}

	for i := 0; i < nReduce; i++ {
		task := Task{
			id:        i,
			taskState: READY,
		}
		c.reduceTasks[i] = task
	}
	
	fmt.Println("MTASKS, RTASKS", len(c.mapTasks), len(c.reduceTasks))

	go c.ReAssignTimeoutTasks()

	c.server()
	return &c
}
