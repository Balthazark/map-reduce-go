package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
    for {
        // Step 1: Send an RPC to the coordinator asking for a task
        args := TaskRequestArgs{} 
        var reply TaskRequestReply 
        ok := call("Coordinator.GetTask", &args, &reply)
        if !ok {
            break // Assuming coordinator has exited, worker can terminate
        }

        // Step 2: Check the type of task received and perform the corresponding action
        if reply.TaskType == MapTask {
            // Step 3: Read the intermediate file for the Map task
            intermediate := readIntermediateFile(reply.FileName)

            // Step 4: Call the application Map function
            output := mapf(reply.FileName, intermediate)

            // Step 5: Send the Map output back to the coordinator
            mapOutputArgs := MapOutputArgs{
                TaskType: MapTask,
                FileName: reply.FileName,
                Output:   output,
            }
            var mapOutputReply MapOutputReply
            ok := call("Coordinator.MapTaskCompleted", &mapOutputArgs, &mapOutputReply)
            if !ok {
                break // Assuming coordinator has exited, worker can terminate
            }
        } else {
            // Handle Reduce task
            // Similar steps as above, but for Reduce task
        }
    }
}

// readIntermediateFile reads the content of an intermediate file associated with a Map task.
func readIntermediateFile(fileName string) string {
    file, err := os.Open(fileName)
    if err != nil {
        log.Fatalf("cannot open intermediate file %v", fileName)
    }
    defer file.Close()

    content, err := io.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read intermediate file %v", fileName)
    }

    return string(content)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
