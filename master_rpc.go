// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
)

// startRPCServer initializes and starts the RPC server for the master node.
// It handles worker registration and task distribution through RPC calls.
//
// The server setup process:
// 1. Validates the master address
// 2. Creates a new RPC server instance
// 3. Registers the master's RPC methods
// 4. Sets up a Unix domain socket listener
// 5. Starts accepting connections in a separate goroutine
//
// Error handling:
//   - Fatally exits if master address is empty
//   - Creates parent directory if it doesn't exist
//   - Retries listener creation after directory creation
func (mr *Master) startRPCServer() {
	if mr.address == "" {
		log.Fatalf("Master address cannot be empty")
	}

	// Create a new RPC server and register master's methods
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(mr); err != nil {
		log.Fatalf("Register masterRPC server failed with error: %v\n", err)
	}

	// Clean up any existing socket file
	os.Remove(mr.address)

	log.Printf("Starting RPC server at: %s", mr.address)

	// Attempt to create the listener
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Printf("Failed to listen on %s: %v\n", mr.address, e)
		// Try to create parent directory if it doesn't exist
		if dir := filepath.Dir(mr.address); dir != "" {
			if err := os.MkdirAll(dir, 0777); err != nil {
				log.Fatalf("Failed to create directory %s: %v", dir, err)
			}
			// Try listening again after creating directory
			l, e = net.Listen("unix", mr.address)
			if e != nil {
				log.Fatalf("Listen master rpc address with error: %v\n", e)
			}
		}
	}
	mr.listener = l

	// Start accepting connections in a separate goroutine
	go func() {
		for {
			select {
			case <-mr.shutdown:
				return
			default:
			}
			conn, err := mr.listener.Accept()
			if err != nil {
				log.Printf("RPC server accept error: %v", err)
				return
			}
			go func() {
				rpcServer.ServeConn(conn)
				conn.Close()
			}()
		}
	}()
}

// Shutdown handles the graceful shutdown of the master's RPC server.
// It closes the listener and logs the shutdown event.
//
// Parameters:
//   - _: Unused arguments required by RPC interface
//
// Returns:
//   - error: Always returns nil as shutdown errors are logged but not propagated
func (mr *Master) Shutdown(_, _ *struct{}) error {
	log.Printf("Shutdown: registration server\n")
	mr.listener.Close()
	return nil
}

// stopRPCServer initiates the shutdown of the RPC server by making a shutdown RPC call.
// This ensures a clean shutdown of the server and its connections.
//
// The function:
// 1. Makes a shutdown RPC call to the master
// 2. Logs any failures during shutdown
// 3. Prints confirmation of successful shutdown
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if !ok {
		log.Fatalf("RPC: Stop failed!!!\n")
	}
	fmt.Println("stop registration down")
}
