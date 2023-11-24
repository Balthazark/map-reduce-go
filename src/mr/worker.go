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
	"path/filepath"
	"sort"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		response, err := CallGetTask()

		if err != nil {
			fmt.Println(err)
		}

		if response.TaskType == MAP {
			handleMapTask(response.File, response.Id, response.NReduce, mapf)
			CallCompleteTask(response.TaskType, response.Id)

		} else if response.TaskType == REDUCE {
			handleReduceTask(response.Id, reducef)
			CallCompleteTask(response.TaskType, response.Id)
		}
	}

}

// Handlers functions
func handleMapTask(fileName string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatalf("Failed to open file in map handler %v", fileName)
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
		encoder := json.NewEncoder(intermediateFile)

		encoderErr := encoder.Encode(keyValue)

		if encoderErr != nil {
			log.Fatal("Failed to encode key value")
		}

	}

}

func handleReduceTask(taskId int, reducef func(string, []string) string) {
	files, err := filepath.Glob(fmt.Sprintf("m-*-%d", taskId))

	if err != nil {
		log.Fatal("Could not find files")
	}

	keyValues := []KeyValue{}
	var keyValue KeyValue

	for _, filePath := range files {
		file, openErr := os.Open(filePath)

		if openErr != nil {
			log.Fatalf("Could not open files in reduce handler")
		}

		decoder := json.NewDecoder(file)

		for decoder.More() {
			decodeErr := decoder.Decode(&keyValue)

			if decodeErr != nil {
				log.Fatal("Could not decode")
			}

			keyValues = append(keyValues, keyValue)
		}
	}

	sort.Sort(ByKey(keyValues))

	outputFile, createErr := os.Create(fmt.Sprintf("m-out-%d", taskId))

	if createErr != nil {
		log.Fatal("Could not create file")
	}

	keyValueMap := map[string][]string{}

	i := 0
	for i < len(keyValues) {
		key := keyValues[i].Key
		keyValueMap[key] = append(keyValueMap[key], keyValues[i].Value)

		// Check if there are more elements in the slice before accessing keyValues[i+1]
		if i+1 < len(keyValues) && key == keyValues[i+1].Key {
			for {
				i++
				if i+1 < len(keyValues) && key == keyValues[i+1].Key {
					keyValueMap[key] = append(keyValueMap[key], keyValues[i].Value)
				} else {
					break
				}
			}
		} else {
			i++
		}
		fmt.Println("KEY VALUES MAP")
		fmt.Printf("%+v\n", keyValueMap)
	}

	for key, value := range keyValueMap {

		output := reducef(key, value)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

	outputFile.Close()
}

func CallCompleteTask(task TaskType, id int) error {
	args := TaskDoneArgs{
		TaskId:   id,
		TaskType: task,
	}
	response := TaskDoneResponse{}

	ok := call("Coordinator.TaskComplete", &args, &response)
	if ok {
		return nil
	} else {
		log.Fatal("Call failed")
	}

	return nil
}

// RPC call functions
func CallGetTask() (*TaskResponse, error) {
	args := TaskArgs{}

	response := TaskResponse{}

	ok := call("Coordinator.GetTask", &args, &response)
	if ok {
		fmt.Printf("call succeeded \n")
		fmt.Println("response in call function", &response)
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
