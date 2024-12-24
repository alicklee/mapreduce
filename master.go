package mapreduce

import (
	"net"
	"sync"
)

type Master struct {
	address string // master node address
	sync.Mutex
	workers  []string // workers RPC address cache
	jobName  jobParse // job name
	files    []string // input files
	nReduce  int      // the number of reduce node
	newCond  *sync.Cond
	listener net.Listener  // master RPC listener
	shutdown chan struct{} // shutdown listener
}

// newMaster creates a new Master instance.
func newMaster(master string) *Master {
	mr := &Master{}
	mr.newCond = sync.NewCond(mr)
	mr.address = master
	mr.shutdown = make(chan struct{})

	return mr
}

// Sequential executes a MapReduce job sequentially, useful for debugging or single-node environments.
// Parameters:
// - jobName: The name of the current job, used to distinguish between different MapReduce tasks.
// - files: A list of input files, each serving as input for the Map phase.
// - nReduce: The number of Reduce tasks, determining the level of parallelism in the Reduce phase.
// - mapF: A user-defined Map function to process input files and generate intermediate key-value pairs.
// - reduceF: A user-defined Reduce function to process intermediate key-value pairs and generate final results.
func Sequential(
	jobName jobParse,
	files []string,
	nReduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) {
	// Initialize Master to manage tasks
	master := newMaster("master")

	// Schedule map tasks
	master.run(jobName, files, nReduce, func(phase jobParse) {
		switch phase {
		case mapParse:
			master.runMapTasks(mapF)
		case reduceParse:
			master.runReduceTasks(reduceF)
		}
	})
}

// runMapTasks executes all Map tasks.
func (mr *Master) runMapTasks(mapF func(string, string) []KeyValue) {
	for i, file := range mr.files {
		doMap(mr.jobName, i, file, mr.nReduce, mapF)
	}
}

// runReduceTasks executes all Reduce tasks.
func (mr *Master) runReduceTasks(reduceF func(string, []string) string) {
	nFiles := len(mr.files)
	for i := 0; i < mr.nReduce; i++ {
		doReduce(mr.jobName, i, mergeName(mr.jobName, i), nFiles, reduceF)
	}
}

// run schedules the Map and Reduce tasks in sequence.
func (mr *Master) run(
	jobName jobParse,
	files []string,
	nReduce int,
	schedule func(phase jobParse),
) {
	mr.files = files
	mr.nReduce = nReduce
	mr.jobName = jobName
	// Execute the Map phase
	schedule(mapParse)
	// Execute the Reduce phase
	schedule(reduceParse)
	// merge files
	mr.merge()
}

// This is worker register RPC function
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	// worker registe to master
	mr.workers = append(mr.workers, args.Worker)
	// Broadcast to other nodes there is new worker node register
	mr.newCond.Broadcast()
	return nil
}
