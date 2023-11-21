package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskArgs struct {
}

type TaskResponse struct {
	file     string
	taskType TaskType
}

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

type MapTask struct {
	file      string
	timeStamp time.Time
	taskState TaskState
	taskType  TaskType
}

type ReduceTask struct {
	intermediateFile string
	timeStamp        time.Time
	taskState        TaskState
	taskType         TaskType
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
