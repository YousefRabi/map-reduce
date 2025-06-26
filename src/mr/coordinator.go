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
	// Channels for coordinating task availability
	mapTaskAvailable    chan struct{}
	reduceTaskAvailable chan struct{}
	jobDone             chan struct{}
	nReduce             int
	mu                  sync.Mutex
}

// Worker uses this to ask for a task.
func (c *Coordinator) GetTask(args *EmptyArgs, reply *Task) error {
	for {
		if c.Done() {
			reply.TaskType = EXIT
			return nil
		}

		// Try get a map task
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

		allMapsDone := len(c.mapInProgress) == 0
		c.mu.Unlock()

		if allMapsDone {
			// Try get a reduce task
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

			// No reduce tasks available, wait for notification
			select {
			case <-c.reduceTaskAvailable:
				continue
			case <-c.jobDone:
				reply.TaskType = EXIT
				return nil
			}
		} else {
			// Maps still in progress, wait for notification
			select {
			case <-c.mapTaskAvailable:
				continue
			case <-c.reduceTaskAvailable:
				continue
			}
		}

	}
}

// Worker uses this to mark a map task as completed.
func (c *Coordinator) CompleteMapTask(args *Task, reply *EmptyArgs) error {
	taskNum := args.TaskNum
	c.mu.Lock()
	if _, ok := c.mapInProgress[taskNum]; !ok {
		return fmt.Errorf("worker said map taskNum %v completed when it was not even in progress", taskNum)
	}
	delete(c.mapInProgress, taskNum)

	if len(c.mapPending) == 0 && len(c.mapInProgress) == 0 {
		select {
		case c.reduceTaskAvailable <- struct{}{}:
		default: // nonblocking
		}
	}
	c.mu.Unlock()
	return nil
}

// Worker uses this to mark a reduce task as completed.
func (c *Coordinator) CompleteReduceTask(args *Task, reply *EmptyArgs) error {
	taskNum := args.TaskNum
	c.mu.Lock()
	if _, ok := c.reduceInProgress[taskNum]; !ok {
		return fmt.Errorf("worker said %v taskNum reduce completed when it was not even in progress", taskNum)
	}
	c.reduceCompleted[taskNum] = true
	delete(c.reduceInProgress, taskNum)
	c.mu.Unlock()

	if c.Done() {
		close(c.jobDone)
	}
	return nil
}

// Coordinator uses this to track whether a task finishes within 10 seconds
// to assert worker's health. If it doesn't, it gets moved back to the pending
// pool to give to another worker.
func (c *Coordinator) track(task Task) error {
	time.Sleep(10 * time.Second)
	switch task.TaskType {
	case MAP:
		c.mu.Lock()
		if _, ok := c.mapInProgress[task.TaskNum]; ok {
			fmt.Printf("%v task num %v %v took longer than 10s, rescheduling it...\n", task.TaskType, task.TaskNum, task.Filename)
			c.mapPending[task.TaskNum] = task.Filename
			delete(c.mapInProgress, task.TaskNum)

			select {
			case c.mapTaskAvailable <- struct{}{}:
			default:
			}
		}
		c.mu.Unlock()
	case REDUCE:
		c.mu.Lock()
		if _, ok := c.reduceInProgress[task.TaskNum]; ok {
			fmt.Printf("%v task num %v %v took longer than 10s, rescheduling it...\n", task.TaskType, task.TaskNum, task.Filename)
			c.reducePending[task.TaskNum] = true
			delete(c.reduceInProgress, task.TaskNum)

			select {
			case c.reduceTaskAvailable <- struct{}{}:
			default:
			}
		}
		c.mu.Unlock()
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
	c.mu.Lock()
	ret := len(c.mapPending) == 0 &&
		len(c.mapInProgress) == 0 &&
		len(c.reducePending) == 0 &&
		len(c.reduceInProgress) == 0 &&
		len(c.reduceCompleted) == c.nReduce
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapPending = make(map[int]string)
	c.mapInProgress = make(map[int]string)
	c.reducePending = make(map[int]bool)
	c.reduceInProgress = make(map[int]bool)
	c.reduceCompleted = make(map[int]bool)
	c.nReduce = nReduce
	c.mapTaskAvailable = make(chan struct{}, 1)
	c.reduceTaskAvailable = make(chan struct{}, 1)
	c.jobDone = make(chan struct{})

	for i, file := range files {
		c.mapPending[i] = file
	}

	for i := range nReduce {
		c.reducePending[i] = true
	}

	c.server()
	return &c
}
