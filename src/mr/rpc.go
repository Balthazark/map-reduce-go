package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskState string

const (
	READY     TaskState = "READY"
	BUSY      TaskState = "BUSY"
	COMPLETED TaskState = "COMPLETED"
	CANCELED  TaskState = "CANCELED"
)

type TaskType string

const (
	MAP    TaskType = "MAP"
	REDUCE TaskType = "REDUCE"
)

type TaskArgs struct {
}

// Capitalize fields to solve export error
type TaskResponse struct {
	File     string
	Id       int
	TaskType TaskType
	NReduce  int
}

type TaskDoneArgs struct {
	TaskId   int
	TaskType TaskType
}

type TaskDoneResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
