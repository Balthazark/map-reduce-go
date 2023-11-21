package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

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

type TaskType int

const (
    MapTask    TaskType = 0
    ReduceTask TaskType = 1
)

type TaskRequestArgs struct {
    // Add any necessary fields for task request
}

type TaskRequestReply struct {
    TaskType TaskType
    FileName string
}

type MapOutputArgs struct {
    TaskType TaskType
    FileName string
    Output   []KeyValue
}

type MapOutputReply struct {
    // Add any necessary fields for Map output acknowledgment
}


// Add your RPC definitions here.
// MapTaskCompleted handles completed Map tasks.
func (c *Coordinator) MapTaskCompleted(args *MapOutputArgs, reply *MapOutputReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Step 3: Implement logic to handle completed Map task
    // Update the status of the completed Map task, e.g., mark it as completed
    for i, task := range c.mapTasks {
        if task.FileName == args.FileName {
            // Mark the Map task as completed
            c.mapTasks[i].Completed = true
            break
        }
    }

    // Step 4: Set reply fields accordingly (if needed)
    // ...

    return nil
}

func (c *Coordinator) GetTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
	var assignedTaskType TaskType
	var assignedFileName string
    // Step 1: Implement logic to assign Map or Reduce tasks to workers
    if len(c.mapTasks) > 0 {
        // Assign a Map task to the worker
        task := c.mapTasks[0]
        c.mapTasks = c.mapTasks[1:] // Remove assigned Map task
        assignedTaskType = MapTask
        assignedFileName = task.FileName
    } else if len(c.reduceTasks) > 0 {
        // Assign a Reduce task to the worker
        task := c.reduceTasks[0]
        c.reduceTasks = c.reduceTasks[1:] // Remove assigned Reduce task
        assignedTaskType = ReduceTask
        assignedFileName = task.FileName
    } else {
        // No more tasks to assign, worker can exit
        return errors.New("no tasks to assign, worker can exit")
    }

    // Step 2: Set reply fields accordingly
    reply.TaskType = assignedTaskType
    reply.FileName = assignedFileName

    return nil
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
