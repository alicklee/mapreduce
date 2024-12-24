// Package mapreduce provides RPC-related types and utilities for the MapReduce framework.
package mapreduce

import (
	"fmt"
	"net/rpc"
)

// RegisterArgs represents the arguments for worker registration RPC.
type RegisterArgs struct {
	Worker string // Network address of the registering worker
}

// DoTaskArgs encapsulates the arguments needed for task execution RPCs.
type DoTaskArgs struct {
	JobName    jobParse // Name of the MapReduce job
	File       string   // Input file for Map task or intermediate file for Reduce task
	Phase      jobParse // Current phase (Map or Reduce)
	TaskNumber int      // Index of the current task

	// For reduce tasks, represents the number of map tasks that generated
	// intermediate files. For map tasks, represents the number of reduce tasks
	// that will process the intermediate results.
	OtherTaskNumber int
}

// ShutdownReply contains the response data for worker shutdown RPC.
type ShutdownReply struct {
	Ntasks int // Number of tasks completed by the worker before shutdown
}

// call performs an RPC call to the specified service.
// Parameters:
//   - srv: Network address of the RPC server
//   - rpcName: Name of the RPC method to call
//   - args: Arguments to pass to the RPC method
//   - reply: Pointer to store the RPC response
//
// Returns true if the RPC call was successful, false otherwise.
func call(srv string, rpcName string, args interface{}, reply interface{}) bool {
	// Establish RPC connection using Unix domain socket
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	// Perform the RPC call
	if err := c.Call(rpcName, args, reply); err != nil {
		return false
	}
	return true
}

// validateRPCArgs checks if the common RPC arguments are valid.
// This is a helper function to ensure RPC arguments meet basic requirements.
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
