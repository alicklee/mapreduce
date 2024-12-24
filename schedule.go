package mapreduce

import (
	"fmt"
	"sync"
)

// The scheduling function determines how the master assings tasks to workers
func schedule(
	jobName jobParse,
	mapFiles []string,
	nReduce int,
	phase jobParse,
	registerChan chan string,
) {
	// job number
	var nTasks int
	var nOtherTasks int
	var wg sync.WaitGroup

	switch phase {
	case mapParse:
		nTasks = len(mapFiles)
		nOtherTasks = nReduce
	case reduceParse:
		nTasks = nReduce
		nOtherTasks = len(mapFiles)
	}

	var lock *sync.Mutex = &sync.Mutex{}
	tasks := make([]int, nTasks)

	for i := 0; i < nTasks; i++ {
		tasks[i] = i
	}

	for {
		lock.Lock()
		// if no job then break
		if len(tasks) <= 0 {
			lock.Unlock()
			break
		}

		// do task job
		task := tasks[0]
		// delete task has done
		tasks = tasks[1:]
		lock.Unlock()
		doTaskArgs := &DoTaskArgs{
			JobName:         jobName,
			Phase:           phase,
			TaskNumber:      task,
			OtherTaskNumber: nOtherTasks,
		}

		if phase == mapParse {
			doTaskArgs.File = mapFiles[task]
		}

		worker := <-registerChan
		wg.Add(1)

		go func() {
			ok := call(worker)
			if ok {
				wg.Done()
			} else {
			}
		}()
	}

	wg.Wait()
	fmt.Printf("schedule: %v phase done\n", phase)
}
