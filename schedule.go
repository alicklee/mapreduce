package mapreduce

import (
	"fmt"
	"sync"
)

// schedule manages task distribution to workers in a MapReduce job.
// It handles both Map and Reduce phases by assigning tasks to available workers
// and ensuring all tasks complete successfully.
//
// Parameters:
//   - jobName: The name/identifier of the MapReduce job
//   - mapFiles: List of input files for map tasks
//   - nReduce: Number of reduce tasks
//   - phase: Current execution phase (map or reduce)
//   - registerChan: Channel providing available worker addresses
func schedule(
	jobName jobParse,
	mapFiles []string,
	nReduce int,
	phase jobParse,
	registerChan chan string,
) {
	var (
		nTasks      int // Total number of tasks for current phase
		nOtherTasks int // Number of tasks for the other phase
		wg          sync.WaitGroup
		mu          sync.Mutex // Protects the tasks slice
	)

	// Determine task counts based on the current phase
	if phase == mapParse {
		nTasks, nOtherTasks = len(mapFiles), nReduce
	} else {
		nTasks, nOtherTasks = nReduce, len(mapFiles)
	}

	// Initialize task queue
	tasks := initTasks(nTasks)

	// Process tasks until all are completed
	for len(tasks) > 0 {
		taskNum := getNextTask(&mu, &tasks)
		worker := <-registerChan
		wg.Add(1)

		go executeTask(
			taskContext{
				worker:      worker,
				taskNum:     taskNum,
				phase:       phase,
				jobName:     jobName,
				mapFiles:    mapFiles,
				nOtherTasks: nOtherTasks,
			},
			&wg,
			&mu,
			&tasks,
			registerChan,
		)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

type taskContext struct {
	worker      string
	taskNum     int
	phase       jobParse
	jobName     jobParse
	mapFiles    []string
	nOtherTasks int
}

func initTasks(nTasks int) []int {
	tasks := make([]int, nTasks)
	for i := range tasks {
		tasks[i] = i
	}
	return tasks
}

func getNextTask(mu *sync.Mutex, tasks *[]int) int {
	mu.Lock()
	defer mu.Unlock()

	taskNum := (*tasks)[0]
	*tasks = (*tasks)[1:]
	return taskNum
}

func executeTask(
	ctx taskContext,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	tasks *[]int,
	registerChan chan string,
) {
	// Prepare task arguments
	taskArgs := &DoTaskArgs{
		JobName:         ctx.jobName,
		Phase:           ctx.phase,
		TaskNumber:      ctx.taskNum,
		OtherTaskNumber: ctx.nOtherTasks,
	}

	// Set input file for map tasks
	if ctx.phase == mapParse {
		taskArgs.File = ctx.mapFiles[ctx.taskNum]
	}

	// Execute the task
	success := call(ctx.worker, DoTaskMethod, taskArgs, new(struct{}))

	if success {
		wg.Done()
		// Return worker to the pool
		go func() { registerChan <- ctx.worker }()
	} else {
		// Handle task failure
		mu.Lock()
		*tasks = append(*tasks, ctx.taskNum)
		mu.Unlock()
		wg.Done()

		fmt.Printf("Task failed: phase=%v, task=%d, worker=%s\n",
			ctx.phase, ctx.taskNum, ctx.worker)
	}
}
