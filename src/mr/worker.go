package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "io"
import "path/filepath"
import "sort"
import "strconv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	emptyArgs := EmptyArgs{}
	reduceTasksReply := ReduceTasksReply{}
	ok := call("Coordinator.GetReduceN", &emptyArgs, &reduceTasksReply)

	if !ok {
		log.Fatalf("get reduce n rpc call failed")
	}

	nReduce := reduceTasksReply.NReduce

	for {
		reply := Task{}

		ok := call("Coordinator.GetTask", &emptyArgs, &reply)

		filename := reply.Filename

		if !ok {
			log.Fatalf("get task rpc call failed %v", filename)
		}

		switch reply.TaskType {
		// Read contents of a file, execute map on it, partition results
		// into nReduce buckets and save each in an intermediate file
		// named mr-X-Y where X and Y are map task num and reduce task num respectively
		case MAP:
			// Open input file and execute map on its contents
			// to get a []mr.KeyValue
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// Write map results to intermediate file mr-X-Y described above
			taskNum := reply.TaskNum
			intermediateFilesContents := make(map[int][]KeyValue)
			for _, kv := range kva {
				reduceTask := ihash(kv.Key) % nReduce
				intermediateFilesContents[reduceTask] = append(intermediateFilesContents[reduceTask], kv)
			}

			for reduceTask, contents := range intermediateFilesContents {
				intermediateFilename := fmt.Sprintf("mr-%v-%v", taskNum, reduceTask)
				intermediateFile, err := os.CreateTemp(".", intermediateFilename+"-*")
				if err != nil {
					log.Fatalf("cannot create %v", intermediateFilename)
				}
				enc := json.NewEncoder(intermediateFile)
				for _, kv := range contents {
					err := enc.Encode(&kv)

					if err != nil {
						log.Fatalf("cannot encode %v", intermediateFilename)
					}
				}
				intermediateFile.Close()
				os.Rename(intermediateFile.Name(), intermediateFilename)
			}

			// Ask Coordinator to mark task as completed
			taskArgs := Task{Filename: filename, TaskNum: taskNum}
			ok := call("Coordinator.CompleteMapTask", &taskArgs, &emptyArgs)

			if !ok {
				log.Fatalf("complete map task rpc call failed %v", filename)
			}
		// Reduce finds all files related to a reduce task based on suffix mr-X-Y with Y
		// reduce task num. It then reads contents of these files, collates values related
		// to a single key, and applies the reduce function on the values for each key.
		// It saves the output for a reduce task in mr-out-Y where Y is the reduce task number.
		case REDUCE:
			pattern := "mr-*-" + strconv.Itoa(reply.TaskNum)
			matches, err := filepath.Glob(pattern)

			if err != nil {
				log.Fatalf("error fetching reduce files for %v", reply.TaskNum)
			}

			kva := []KeyValue{}
			for _, match := range matches {
				file, err := os.Open(match)
				if err != nil {
					log.Fatalf("cannot open %v", match)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
			ofile, _ := os.CreateTemp(".", oname+"-*")
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
			// Atomically rename file to oname after it's finished to avoid
			// writing nonfinished files in case of crashes
			os.Rename(ofile.Name(), oname)

			// Ask Coordinator to mark task as completed
			taskArgs := Task{TaskNum: reply.TaskNum}
			ok := call("Coordinator.CompleteReduceTask", &taskArgs, &emptyArgs)

			if !ok {
				log.Fatalf("complete reduce task rpc call failed %v", filename)
			}

		case EXIT:
			log.Printf("all tasks are finished, exiting...")
			return
		default:
			log.Printf("unknown task type %v", reply.TaskType)
		}
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
