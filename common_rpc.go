// Package mapreduce provides RPC-related types and utilities
// for communication between master and worker nodes.
package mapreduce

import (
	"context"
	"fmt"
	"net/rpc"
	"time"
)

// Constants for RPC method names used throughout the system
const (
	// RegisterMethod is used when a worker registers with the master
	RegisterMethod = "Master.Register"
	// DoTaskMethod is called when assigning a task to a worker
	DoTaskMethod = "Worker.DoTask"
	// ShutdownMethod is invoked to gracefully terminate a worker
	ShutdownMethod = "Worker.Shutdown"
)

// RegisterArgs represents the arguments for worker registration RPC.
// Worker field contains the network address of the registering worker.
type RegisterArgs struct {
	Worker string
}

// DoTaskArgs encapsulates all necessary information for task execution RPCs.
type DoTaskArgs struct {
	JobName    jobParse // Unique identifier for the MapReduce job
	File       string   // File to process: input file for Map, intermediate file for Reduce
	Phase      jobParse // Current execution phase (Map or Reduce)
	TaskNumber int      // Task identifier within the current phase

	// OtherTaskNumber serves dual purpose:
	// - For reduce tasks: number of map tasks that generated intermediate files
	// - For map tasks: number of reduce tasks that will process the results
	OtherTaskNumber int
}

// ShutdownReply contains the response data for worker shutdown RPC.
// Ntasks represents the total number of tasks completed by the worker
// before shutdown.
type ShutdownReply struct {
	Ntasks int
}

// call performs an RPC call to the specified service with timeout control.
// Parameters:
//   - srv: Network address of the RPC server (unix socket path)
//   - rpcName: Name of the RPC method to invoke
//   - args: Arguments to pass to the RPC method
//   - reply: Pointer to store the RPC response
//
// Returns:
//   - bool: true if the RPC call was successful, false if it failed or timed out
func call(srv string, rpcName string, args interface{}, reply interface{}) bool {
	if err := validateRPCArgs(srv, rpcName, args); err != nil {
		return false
	}
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	// Set up timeout context to prevent indefinite blocking
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create buffered channel for RPC response
	done := make(chan error, 1)

	// Execute RPC call in separate goroutine to enable timeout control
	go func() {
		done <- c.Call(rpcName, args, reply)
	}()

	// Wait for either RPC completion or timeout
	select {
	case err := <-done:
		// Return true only if RPC completed without error
		return err == nil
	case <-ctx.Done():
		// Return false if timeout occurred
		return false
	}
}

// validateRPCArgs performs basic validation of RPC call parameters.
// Returns an error if any of the required fields are empty or nil.
func validateRPCArgs(srv, rpcName string, args interface{}) error {
	if srv == "" {
		return fmt.Errorf("server address cannot be empty")
	}
	if rpcName == "" {
		return fmt.Errorf("RPC method name cannot be empty")
	}
	if args == nil {
		return fmt.Errorf("RPC arguments cannot be nil")
	}
	return nil
}
