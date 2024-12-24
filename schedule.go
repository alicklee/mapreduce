// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"sync"
	"time"
)

// schedule manages task distribution and execution in a MapReduce job.
// It coordinates the assignment of map or reduce tasks to available workers
// and ensures all tasks complete successfully.
//
// The scheduling process works as follows:
// 1. Determines the number of tasks based on the current phase
// 2. Creates a channel to distribute task numbers
// 3. Assigns tasks to available workers as they register
// 4. Handles task failures by re-queueing failed tasks
// 5. Waits for all tasks to complete before proceeding
//
// Parameters:
//   - jobName: Unique identifier for the MapReduce job
//   - mapFiles: List of input files for map tasks
//   - nReduce: Number of reduce tasks to create
//   - phase: Current execution phase (map or reduce)
//   - registerChan: Channel providing available worker addresses
type taskContext struct {
	worker      string   // Address of the worker to execute the task
	taskNum     int      // Task number within the current phase
	phase       jobParse // Current phase (map or reduce)
	jobName     jobParse // Name of the MapReduce job
	mapFiles    []string // Input files for map phase
	nOtherTasks int      // Number of tasks in the other phase
}

func schedule(
	jobName jobParse,
	mapFiles []string,
	nReduce int,
	phase jobParse,
	registerChan chan string,
) {
	var (
		nTasks      int
		nOtherTasks int
		wg          sync.WaitGroup
	)

	if phase == mapParse {
		nTasks, nOtherTasks = len(mapFiles), nReduce
	} else {
		nTasks, nOtherTasks = nReduce, len(mapFiles)
	}

	// Create buffered channels for task distribution
	taskChan := make(chan int, nTasks)
	failedTasks := make(chan int, nTasks)

	// Initialize task channel
	for i := 0; i < nTasks; i++ {
		taskChan <- i
	}

	done := make(chan struct{})
	taskCount := nTasks // Track remaining tasks
	var mu sync.Mutex   // Protect taskCount

	go func() {
		for {
			select {
			case taskNum, ok := <-taskChan:
				if !ok {
					close(done)
					return
				}
				worker := <-registerChan
				wg.Add(1)

				go func(taskNum int, worker string) {
					defer wg.Done()
					success := false
					maxRetries := 5

					// Implement exponential backoff retry
					for retries := 0; retries < maxRetries && !success; retries++ {
						success = executeTask(taskContext{
							worker:      worker,
							taskNum:     taskNum,
							phase:       phase,
							jobName:     jobName,
							mapFiles:    mapFiles,
							nOtherTasks: nOtherTasks,
						})

						if success {
							mu.Lock()
							taskCount--
							if taskCount == 0 {
								close(taskChan)
								close(failedTasks)
							}
							mu.Unlock()
							break
						}

						if retries < maxRetries-1 {
							backoff := time.Duration(1<<uint(retries)) * 100 * time.Millisecond
							time.Sleep(backoff)
						}
					}

					if !success {
						select {
						case failedTasks <- taskNum:
							// Task queued for retry
						case <-done:
							// System is shutting down
						}
					}
					registerChan <- worker
				}(taskNum, worker)

			case taskNum, ok := <-failedTasks:
				if !ok {
					continue
				}
				select {
				case taskChan <- taskNum:
					// Task requeued
				case <-done:
					// System is shutting down
					return
				}
			}
		}
	}()

	wg.Wait()
	<-done
}

// executeTask attempts to execute a single task on a worker.
// It prepares the task arguments and makes an RPC call to the worker.
//
// Parameters:
//   - ctx: Context containing all information needed for task execution
//
// Returns:
//   - bool: true if task completed successfully, false if it failed
func executeTask(ctx taskContext) bool {
	// Prepare task arguments for RPC call
	taskArgs := &DoTaskArgs{
		JobName:         ctx.jobName,
		Phase:           ctx.phase,
		TaskNumber:      ctx.taskNum,
		OtherTaskNumber: ctx.nOtherTasks,
	}

	// Set input file for map tasks
	// Reduce tasks don't need an input file specified
	if ctx.phase == mapParse {
		taskArgs.File = ctx.mapFiles[ctx.taskNum]
	}

	// Execute the task via RPC and return success status
	return call(ctx.worker, DoTaskMethod, taskArgs, new(struct{}))
}
