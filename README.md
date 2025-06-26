# Simple Map Reduce System in Go

This is an implementation of Lab 1 for MIT 6.5840 Distributed Systems course in Go.

The original lab assignment can be found [here](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html).

Several map reduce apps are in `src/mrapps`
To build one (e.g. word count app) use:

```
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
```

This will make the map and reduce functions available to the workers via go plugins.

Then, start a coordinator process and one or more worker processes.

The coordinator will serve the workers map or reduce tasks depending on the progress of the system.

Once all tasks are finished, the coordinator and workers will exit.

The output will be in mr-out-$REDUCE_TASK_NUM where the number of reduce tasks is an argument to the coordinator in `src/main/mrcoordinator.go`

To start a coordinator process with some input files, use `go run mrcoordinator.go pg-*.txt`
To start a worker process, use `go run worker.go wc.go`

The workers talk to the coordinator via RPC.

To run the tests, use `bash test-mr.sh` or `bash test-mr-many.sh $NUM_TESTS` from `src/main`

