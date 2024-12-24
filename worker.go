// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker represents a worker node in the MapReduce framework.
// It executes Map and Reduce tasks assigned by the master.
type Worker struct {
	sync.Mutex                                 // Protects concurrent access to worker state
	name       string                          // Unique identifier for this worker
	MapF       func(string, string) []KeyValue // User-defined Map function
	ReduceF    func(string, []string) string   // User-defined Reduce function
	nTasks     int                             // Number of tasks completed by this worker
	listener   net.Listener                    // RPC listener for receiving task assignments
	nRPC       int                             // Number of RPCs remaining before shutdown
}

// DoTask executes a single Map or Reduce task.
// It updates the task counter and processes the task according to its phase.
func (wk *Worker) DoTask(args *DoTaskArgs, _ *struct{}) error {
	wk.Lock()
	wk.nTasks++
	wk.Unlock()

	switch args.Phase {
	case mapParse:
		doMap(args.JobName, args.TaskNumber, args.File, args.OtherTaskNumber, wk.MapF)
	case reduceParse:
		doReduce(
			args.JobName,
			args.TaskNumber,
			mergeName(args.JobName, args.TaskNumber),
			args.OtherTaskNumber,
			wk.ReduceF,
		)
	}

	fmt.Printf("%s:%v task #%d done\n", wk.name, args.Phase, args.TaskNumber)
	return nil
}

// RunWorker initializes and starts a worker node.
// It sets up the RPC server and handles incoming task assignments.
//
// Parameters:
//   - masterAddress: Address of the master node
//   - me: Unique identifier for this worker
//   - mapF: User-defined Map function
//   - reduceF: User-defined Reduce function
//   - nRPC: Maximum number of RPCs to handle before shutdown
func RunWorker(
	masterAddress string,
	me string,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
	nRPC int,
) error {
	wk := &Worker{
		name:    me,
		MapF:    mapF,
		ReduceF: reduceF,
		nRPC:    nRPC,
	}

	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me)
	l, err := net.Listen("unix", me)
	if err != nil {
		return fmt.Errorf("RunWorker: worker %s error: %v", me, err)
	}
	wk.listener = l

	// Register with master before serving
	if err := wk.register(masterAddress); err != nil {
		l.Close()
		return err
	}

	// Serve RPC requests
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				break
			}
			go rpcs.ServeConn(conn)
		}
	}()

	return nil
}

// register notifies the master of this worker's existence
func (wk *Worker) register(master string) error {
	args := &RegisterArgs{Worker: wk.name}
	ok := call(master, RegisterMethod, args, new(struct{}))
	if !ok {
		log.Printf("Register: RPC %s master error\n", master)
		return fmt.Errorf("Register: RPC %s master error", master)
	}
	return nil
}

// Shutdown handles the worker shutdown request from master.
// It returns the total number of tasks completed by this worker.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	fmt.Printf("Shutdown: worker %s stopping\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	return nil
}
