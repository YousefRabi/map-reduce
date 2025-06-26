# Simple Map Reduce System in Go

This is an implementation of a simple map reduce system in Go for MIT 6.5840 Distributed Systems course.

The original lab assignment can be found [here](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html).

The system comprises of one or more worker processes that call application Map and Reduce functions
and handle reading and writing files, and a coordinator process that hands out tasks to workers and copes
with failed workers.

The workers communicate with the coordinator via RPC.

Once all tasks are finished, the coordinator and workers will exit.

The output will be in mr-out-$REDUCE_TASK_NUM where the number of reduce tasks is an argument to the coordinator in `src/main/mrcoordinator.go`.

Several map reduce apps are in `src/mrapps`. To build one (e.g. word count app) use:

```
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
```

This will make the map and reduce functions available to the workers via go plugins.

Then, start a coordinator process and one or more worker processes.

To start a coordinator process with some input files, use `go run mrcoordinator.go pg-*.txt`.

To start a worker process, use `go run worker.go wc.go`

To run the tests, use `bash test-mr.sh` or `bash test-mr-many.sh $NUM_TESTS` from `src/main`

