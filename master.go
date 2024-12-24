// Package mapreduce implements a simple MapReduce framework for distributed computing
// It supports both sequential execution for debugging and distributed parallel execution
package mapreduce

import (
	"fmt"
	"log"
	"net"
	"sync"
)

// Master represents the master node of the MapReduce framework
// responsible for task scheduling and worker management
type Master struct {
	// Configuration
	jobName jobParse // Name of the current MapReduce job
	nReduce int      // Number of reduce tasks to be executed
	address string   // Network address of the master node
	files   []string // List of input files to be processed

	// Synchronization
	sync.Mutex            // Mutex for protecting shared resources
	newCond    *sync.Cond // Condition variable for worker registration notifications

	// Runtime state
	workers  []string      // List of registered worker addresses
	listener net.Listener  // Network listener for RPC server
	shutdown chan struct{} // Channel to signal shutdown to all goroutines
	stats    []int
}

// newMaster creates and initializes a new Master instance
func newMaster(master string) *Master {
	mr := &Master{}
	mr.newCond = sync.NewCond(mr)
	mr.address = master
	mr.shutdown = make(chan struct{})
	return mr
}

// Sequential executes a MapReduce job sequentially, useful for debugging or single-node environments
// Parameters:
//   - jobName: Name of the job to distinguish between different MapReduce tasks
//   - files: List of input files, each serving as input for the Map phase
//   - nReduce: Number of reduce tasks, determining the parallelism level in Reduce phase
//   - mapF: User-defined Map function to process input files and generate intermediate key-value pairs
//   - reduceF: User-defined Reduce function to process intermediate key-value pairs and generate final results
func Sequential(
	jobName jobParse,
	files []string,
	nReduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) error {
	if len(files) == 0 {
		return fmt.Errorf("no input files provided")
	}
	if nReduce <= 0 {
		return fmt.Errorf("invalid number of reduce tasks: %d", nReduce)
	}
	if mapF == nil || reduceF == nil {
		return fmt.Errorf("map and reduce functions cannot be nil")
	}

	master := newMaster("master")
	master.run(jobName, files, nReduce, func(phase jobParse) {
		switch phase {
		case mapParse:
			master.runMapTasks(mapF)
		case reduceParse:
			master.runReduceTasks(reduceF)
		}
	}, nil)
	return nil
}

// runMapTasks executes all Map tasks
func (mr *Master) runMapTasks(mapF func(string, string) []KeyValue) {
	for i, file := range mr.files {
		doMap(mr.jobName, i, file, mr.nReduce, mapF)
	}
}

// runReduceTasks executes all Reduce tasks
func (mr *Master) runReduceTasks(reduceF func(string, []string) string) {
	nFiles := len(mr.files)
	for i := 0; i < mr.nReduce; i++ {
		doReduce(mr.jobName, i, mergeName(mr.jobName, i), nFiles, reduceF)
	}
}

// run schedules Map and Reduce tasks in sequence
func (mr *Master) run(
	jobName jobParse,
	files []string,
	nReduce int,
	schedule func(phase jobParse),
	finish func(),
) {
	defer mr.cleanup()

	mr.files = files
	mr.nReduce = nReduce
	mr.jobName = jobName

	schedule(mapParse)
	schedule(reduceParse)
	finish()
	mr.merge()
}

// Register handles worker registration RPC requests
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	if args == nil || args.Worker == "" {
		return fmt.Errorf("invalid worker registration arguments")
	}

	mr.Lock()
	defer mr.Unlock()

	mr.workers = append(mr.workers, args.Worker)
	mr.newCond.Broadcast()
	return nil
}

// forwardRegistration forwards registered worker information to the scheduler
func (mr *Master) forwardRegistration(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			w := mr.workers[i]
			i++ // Increment index before unlocking
			go func(worker string) {
				ch <- worker // Use parameter to avoid race condition
			}(w)
		} else {
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Distributed executes MapReduce tasks in distributed mode
// Parameters:
//   - jobName: Name of the job
//   - files: List of input files
//   - nReduce: Number of reduce tasks
//   - master: Master node identifier
func Distributed(jobName jobParse, files []string, nReduce int, master string) (mr *Master) {
	mr = &Master{
		jobName: jobName,
		files:   files,
		nReduce: nReduce,
	}

	mr.startRPCServer() // Start RPC server

	// Execute job scheduling
	go mr.run(mr.jobName, mr.files, mr.nReduce, func(phase jobParse) {
		ch := make(chan string)
		go mr.forwardRegistration(ch)

		schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
	}, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})

	return mr
}

// Add cleanup method
func (mr *Master) cleanup() {
	if mr.listener != nil {
		mr.listener.Close()
	}
	close(mr.shutdown)
}

func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntask := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		fmt.Printf("Master:Shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, ShutdownMethod, new(struct{}), &reply)
		if !ok {
			log.Fatalf("Master:RPC %s Shutdown failed", w)
		}
		ntask = append(ntask, reply.Ntasks)
	}
	return ntask
}
