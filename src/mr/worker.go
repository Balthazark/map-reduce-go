package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"

	"github.com/avast/retry-go/v4"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		response, err := CallGetTask()

		if err != nil {
			fmt.Println(err)
		}

		if response.taskType == MAP {
			handleMapTask()

		} else {

		}
	}

}

// Handlers functions
func handleMapTask(fileName string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatalf("Failed to open file %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", file)
	}
	file.Close()

	keyValues := mapf(fileName, string(content))

	for _, keyValue := range keyValues {
		reduceId := ihash(keyValue.Key) % nReduce
		intermediateFile, err := os.Create(fmt.Sprintf("m-%d-%-d", taskId, reduceId))
		
		if err != nil {
			log.Fatalf("Cannot create file")
		}
		encoder = json.NewEncoder(intermediateFile)

	}



}

// RPC call functions
func CallGetTask() (*TaskResponse, error) {
	args := TaskArgs{}

	response := TaskResponse{}

	ok := call("Coordinator.GetTask", &args, &response)
	if ok {
		fmt.Printf("call succeeded \n")
		return &response, nil
	} else {
		return nil, errors.New("failed to get task during call")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
