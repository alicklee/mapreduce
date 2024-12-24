// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"sync"
	"time"
)

// taskContext contains all information needed for task execution
type taskContext struct {
	worker      string   // Worker address
	taskNum     int      // Task number
	phase       jobParse // Current phase
	jobName     jobParse // Job name
	mapFiles    []string // Input files
	nOtherTasks int      // Number of tasks in other phase
}

// TaskScheduler manages the scheduling and execution of MapReduce tasks
type TaskScheduler struct {
	jobName      jobParse
	mapFiles     []string
	nReduce      int
	phase        jobParse
	registerChan chan string
	taskCount    int
	wg           sync.WaitGroup
	mu           sync.Mutex
}

// NewTaskScheduler creates a new task scheduler instance
func NewTaskScheduler(
	jobName jobParse,
	mapFiles []string,
	nReduce int,
	phase jobParse,
	registerChan chan string,
) *TaskScheduler {
	ts := &TaskScheduler{
		jobName:      jobName,
		mapFiles:     mapFiles,
		nReduce:      nReduce,
		phase:        phase,
		registerChan: registerChan,
	}

	// Set task count based on phase
	if phase == mapParse {
		ts.taskCount = len(mapFiles)
	} else {
		ts.taskCount = nReduce
	}

	return ts
}

// schedule coordinates task distribution and execution
func schedule(
	jobName jobParse,
	mapFiles []string,
	nReduce int,
	phase jobParse,
	registerChan chan string,
) {
	scheduler := NewTaskScheduler(jobName, mapFiles, nReduce, phase, registerChan)
	scheduler.Run()
}

// Run starts the task scheduling process
func (ts *TaskScheduler) Run() {
	// Initialize channels
	taskChan := ts.createTaskChannel()
	failedTasks := make(chan int, ts.taskCount)
	done := make(chan struct{})

	// Start task processor
	go ts.processTasksAsync(taskChan, failedTasks, done)

	// Wait for completion
	ts.wg.Wait()
	<-done
}

// createTaskChannel initializes and populates the task channel
func (ts *TaskScheduler) createTaskChannel() chan int {
	taskChan := make(chan int, ts.taskCount)
	for i := 0; i < ts.taskCount; i++ {
		taskChan <- i
	}
	return taskChan
}

// processTasksAsync handles task distribution and retry logic
func (ts *TaskScheduler) processTasksAsync(
	taskChan chan int,
	failedTasks chan int,
	done chan struct{},
) {
	for {
		select {
		case taskNum, ok := <-taskChan:
			if !ok {
				close(done)
				return
			}
			ts.handleTask(taskNum, taskChan, failedTasks, done)

		case taskNum, ok := <-failedTasks:
			if !ok {
				continue
			}
			ts.requeueFailedTask(taskNum, taskChan, done)
		}
	}
}

// handleTask processes a single task with retries
func (ts *TaskScheduler) handleTask(
	taskNum int,
	taskChan chan int,
	failedTasks chan int,
	done chan struct{},
) {
	worker := <-ts.registerChan
	ts.wg.Add(1)

	go func() {
		defer ts.wg.Done()
		if ts.executeTaskWithRetry(taskNum, worker) {
			ts.markTaskComplete(taskChan, failedTasks)
		} else {
			ts.handleFailedTask(taskNum, failedTasks, done)
		}
		ts.registerChan <- worker
	}()
}

// executeTaskWithRetry attempts to execute a task with exponential backoff
func (ts *TaskScheduler) executeTaskWithRetry(taskNum int, worker string) bool {
	const maxRetries = 5
	for retries := 0; retries < maxRetries; retries++ {
		if success := ts.executeTask(taskNum, worker); success {
			return true
		}

		if retries < maxRetries-1 {
			backoff := time.Duration(1<<uint(retries)) * 100 * time.Millisecond
			time.Sleep(backoff)
		}
	}
	return false
}

// executeTask attempts to execute a single task
func (ts *TaskScheduler) executeTask(taskNum int, worker string) bool {
	ctx := taskContext{
		worker:      worker,
		taskNum:     taskNum,
		phase:       ts.phase,
		jobName:     ts.jobName,
		mapFiles:    ts.mapFiles,
		nOtherTasks: ts.getOtherTaskCount(),
	}
	return executeTask(ctx)
}

// getOtherTaskCount returns the number of tasks in the other phase
func (ts *TaskScheduler) getOtherTaskCount() int {
	if ts.phase == mapParse {
		return ts.nReduce
	}
	return len(ts.mapFiles)
}

// markTaskComplete updates the task counter and closes channels if needed
func (ts *TaskScheduler) markTaskComplete(taskChan, failedTasks chan int) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.taskCount--
	if ts.taskCount == 0 {
		close(taskChan)
		close(failedTasks)
	}
}

// handleFailedTask attempts to requeue a failed task
func (ts *TaskScheduler) handleFailedTask(
	taskNum int,
	failedTasks chan int,
	done chan struct{},
) {
	select {
	case failedTasks <- taskNum:
		// Task queued for retry
	case <-done:
		// System is shutting down
	}
}

// requeueFailedTask attempts to put a failed task back in the main queue
func (ts *TaskScheduler) requeueFailedTask(
	taskNum int,
	taskChan chan int,
	done chan struct{},
) {
	select {
	case taskChan <- taskNum:
		// Task requeued
	case <-done:
		// System is shutting down
	}
}

// executeTask makes an RPC call to execute a task on a worker
func executeTask(ctx taskContext) bool {
	taskArgs := &DoTaskArgs{
		JobName:         ctx.jobName,
		Phase:           ctx.phase,
		TaskNumber:      ctx.taskNum,
		File:            ctx.mapFiles[ctx.taskNum],
		OtherTaskNumber: ctx.nOtherTasks,
	}
	return call(ctx.worker, DoTaskMethod, taskArgs, new(struct{}))
}
