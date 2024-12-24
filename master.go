package mapreduce

type Master struct{}

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
	master := newMaster()

	// Schedule map tasks
	master.run(jobName, files, nReduce, func(phase jobParse) {
		switch phase {
		case mapParse:
			runMapTasks(jobName, files, nReduce, mapF)
		case reduceParse:
			runReduceTasks(jobName, nReduce, len(files), reduceF)
		}
	})
}

// runMapTasks executes all Map tasks.
func runMapTasks(
	jobName jobParse,
	files []string,
	nReduce int,
	mapF func(string, string) []KeyValue,
) {
	for i, file := range files {
		doMap(jobName, i, file, nReduce, mapF)
	}
}

// runReduceTasks executes all Reduce tasks.
func runReduceTasks(
	jobName jobParse,
	nReduce int,
	nFiles int,
	reduceF func(string, []string) string,
) {
	for i := 0; i < nReduce; i++ {
		doReduce(jobName, i, mergeName(jobName, i), nFiles, reduceF)
	}
}

// newMaster creates a new Master instance.
func newMaster() *Master {
	return &Master{}
}

// run schedules the Map and Reduce tasks in sequence.
func (mr *Master) run(
	jobName jobParse,
	files []string,
	nReduce int,
	schedule func(phase jobParse),
) {
	// Execute the Map phase
	schedule(mapParse)
	// Execute the Reduce phase
	schedule(reduceParse)
	// merge files
	mr.merge(jobName, nReduce)
}
