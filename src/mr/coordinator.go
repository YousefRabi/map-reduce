package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// map tasks' keys are task numbers and values are filenames to be processed
	mapPending    map[int]string
	mapInProgress map[int]string
	// reduce tasks' keys are reduce task numbers since
	// each reduce task needs to aggregate data from multiple intermediate files
	// and intermediate files have the format of mr-X-Y where X is the map task num
	// and Y is the reduce task num
	reducePending    map[int]bool
	reduceInProgress map[int]bool
	reduceCompleted  map[int]bool
	nReduce          int
	mu               sync.Mutex
}

// Worker uses this to ask for a task.
func (c *Coordinator) GetTask(args *EmptyArgs, reply *Task) error {
	// If no map tasks are pending and some are in progress, keep waiting
	// in case some time out and become pending again
	for len(c.mapPending) == 0 &&
		len(c.mapInProgress) > 0 {
		time.Sleep(time.Millisecond)
	}

	if c.Done() {
		reply.TaskType = EXIT
		return nil
	}

	c.mu.Lock()
	for taskNum, filename := range c.mapPending {
		reply.Filename = filename
		reply.TaskType = MAP
		reply.TaskNum = taskNum
		c.mapInProgress[taskNum] = filename
		delete(c.mapPending, taskNum)
		go c.track(*reply)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// If no reduce tasks are pending and some are in progress, keep waiting
	// in case some time out and become pending again
	for len(c.reducePending) == 0 &&
		len(c.reduceInProgress) > 0 {
		time.Sleep(time.Millisecond)
	}

	c.mu.Lock()
	for reduceTask := range c.reducePending {
		reply.TaskType = REDUCE
		reply.TaskNum = reduceTask
		c.reduceInProgress[reduceTask] = true
		delete(c.reducePending, reduceTask)
		go c.track(*reply)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	return nil
}

// Worker uses this to mark a map task as completed.
func (c *Coordinator) CompleteMapTask(args *Task, reply *EmptyArgs) error {
	taskNum := args.TaskNum
	if _, ok := c.mapInProgress[taskNum]; !ok {
		return fmt.Errorf("worker said map taskNum %v completed when it was not even in progress", taskNum)
	}
	c.mu.Lock()
	delete(c.mapInProgress, taskNum)
	c.mu.Unlock()
	return nil
}

// Worker uses this to mark a reduce task as completed.
func (c *Coordinator) CompleteReduceTask(args *Task, reply *EmptyArgs) error {
	taskNum := args.TaskNum
	if _, ok := c.reduceInProgress[taskNum]; !ok {
		return fmt.Errorf("worker said %v taskNum reduce completed when it was not even in progress", taskNum)
	}
	c.mu.Lock()
	c.reduceCompleted[taskNum] = true
	delete(c.reduceInProgress, taskNum)
	c.mu.Unlock()
	return nil
}

// Coordinator uses this to track whether a task finishes within 10 seconds
// to assert worker's health. If it doesn't, it gets moved back to the pending
// pool to give to another worker.
func (c *Coordinator) track(task Task) error {
	time.Sleep(10 * time.Second)
	switch task.TaskType {
	case MAP:
		if _, ok := c.mapInProgress[task.TaskNum]; ok {
			fmt.Printf("%v task num %v %v took longer than 10s, rescheduling it...\n", task.TaskType, task.TaskNum, task.Filename)
			c.mu.Lock()
			c.mapPending[task.TaskNum] = task.Filename
			delete(c.mapInProgress, task.TaskNum)
			c.mu.Unlock()
		}
	case REDUCE:
		if _, ok := c.reduceInProgress[task.TaskNum]; ok {
			fmt.Printf("%v task num %v %v took longer than 10s, rescheduling it...\n", task.TaskType, task.TaskNum, task.Filename)
			c.mu.Lock()
			c.reducePending[task.TaskNum] = true
			delete(c.reduceInProgress, task.TaskNum)
			c.mu.Unlock()
		}
	}
	return nil
}

// Workers use this to get the number of reduce tasks
func (c *Coordinator) GetReduceN(args *EmptyArgs, reply *ReduceTasksReply) error {
	reply.NReduce = c.nReduce
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
	ret := false

	// Your code here.

	ret = len(c.mapPending) == 0 &&
		len(c.mapInProgress) == 0 &&
		len(c.reducePending) == 0 &&
		len(c.reduceInProgress) == 0 &&
		len(c.reduceCompleted) == c.nReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapPending = make(map[int]string)
	c.mapInProgress = make(map[int]string)
	c.reducePending = make(map[int]bool)
	c.reduceInProgress = make(map[int]bool)
	c.reduceCompleted = make(map[int]bool)
	c.nReduce = nReduce

	for i, file := range files {
		c.mapPending[i] = file
	}

	for i := range nReduce {
		c.reducePending[i] = true
	}

	c.server()
	return &c
}
