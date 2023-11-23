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
type TaskState int

const (
	READY = iota
	BUSY
	COMPLETED
	CANCELED
)

type TaskType int

const (
	MAP = iota
	REDUCE
)

type TaskArgs struct {
}

type TaskResponse struct {
	file     string
	id       int
	taskType TaskType
	nReduce  int
}

type TaskDoneResponse struct {
	taskName string
	taskType TaskType
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
