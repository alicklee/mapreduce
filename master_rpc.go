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

// RPCServer manages the RPC service for the master node
type RPCServer struct {
	address  string       // Unix domain socket path
	listener net.Listener // Network listener
	server   *rpc.Server  // RPC server instance
}

// NewRPCServer creates a new RPC server instance
func NewRPCServer(address string) *RPCServer {
	return &RPCServer{
		address: address,
		server:  rpc.NewServer(),
	}
}

// Start initializes and starts the RPC server
func (s *RPCServer) Start(master *Master) error {
	if err := s.validateSetup(); err != nil {
		return err
	}

	if err := s.registerMaster(master); err != nil {
		return err
	}

	if err := s.setupListener(); err != nil {
		return err
	}

	go s.acceptConnections(master.shutdown)
	return nil
}

// validateSetup checks if the server can be started
func (s *RPCServer) validateSetup() error {
	if s.address == "" {
		return fmt.Errorf("master address cannot be empty")
	}
	return nil
}

// registerMaster registers the master's RPC methods
func (s *RPCServer) registerMaster(master *Master) error {
	if err := s.server.Register(master); err != nil {
		return fmt.Errorf("failed to register master: %v", err)
	}
	return nil
}

// setupListener creates and configures the network listener
func (s *RPCServer) setupListener() error {
	// Clean up any existing socket file
	os.Remove(s.address)

	log.Printf("Starting RPC server at: %s", s.address)

	// Create listener
	l, err := s.createListener()
	if err != nil {
		return err
	}

	s.listener = l
	return nil
}

// createListener attempts to create a Unix domain socket listener
func (s *RPCServer) createListener() (net.Listener, error) {
	l, err := net.Listen("unix", s.address)
	if err != nil {
		// Try to create parent directory if it doesn't exist
		if dir := filepath.Dir(s.address); dir != "" {
			if err := os.MkdirAll(dir, 0777); err != nil {
				return nil, fmt.Errorf("failed to create directory %s: %v", dir, err)
			}
			// Retry listener creation
			l, err = net.Listen("unix", s.address)
			if err != nil {
				return nil, fmt.Errorf("failed to create listener: %v", err)
			}
		}
	}
	return l, nil
}

// acceptConnections handles incoming RPC connections
func (s *RPCServer) acceptConnections(shutdown chan struct{}) {
	for {
		select {
		case <-shutdown:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				log.Printf("RPC server accept error: %v", err)
				return
			}
			go s.handleConnection(conn)
		}
	}
}

// handleConnection processes a single RPC connection
func (s *RPCServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	s.server.ServeConn(conn)
}

// Stop gracefully shuts down the RPC server
func (s *RPCServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// startRPCServer is the entry point for starting the master's RPC service
func (mr *Master) startRPCServer() {
	server := NewRPCServer(mr.address)
	if err := server.Start(mr); err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}
	mr.listener = server.listener
}

// Shutdown handles the graceful shutdown of the master's RPC server
func (mr *Master) Shutdown(_, _ *struct{}) error {
	log.Printf("Shutdown: registration server\n")
	return mr.listener.Close()
}

// stopRPCServer initiates the shutdown of the RPC server
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if !ok {
		log.Fatalf("RPC: Stop failed!!!\n")
	}
	fmt.Println("RPC server shutdown complete")
}
