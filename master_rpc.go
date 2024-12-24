package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Open RPC service wait worker registe

func (mr *Master) startRPCServer() {
	// Create a new RPC server
	rpcServer := rpc.NewServer()

	os.Remove(mr.address)

	// register master
	if err := rpcServer.Register(mr); err != nil {
		log.Fatalf("Register masterRPC server failed with error: %v\n", err)
	}

	// listen on master address
	l, err := net.Listen("unix", mr.address)
	if err != nil {
		log.Fatalf("Listen master rpc address with error: %v\n", err)
	}
	mr.listener = l

	go func() {
		for {
			select {
			case <-mr.shutdown:
				return
			default:
			}
			conn, err := mr.listener.Accept()
			if err != nil {
				log.Fatalf("re")
				return
			}
			go func() {
				rpcServer.ServeConn(conn)
				conn.Close()
				fmt.Println("RegisterServer: done!")
			}()
		}
	}()
}

// shutdown master rpc service
func (mr *Master) Shutdown(_, _ *struct{}) error {
	log.Fatalf("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.listener.Close()
	return nil
}

func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	// call RPC request function
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if !ok {
		log.Fatalf("RPC: Stop failed!!!\n")
	}
	fmt.Println("stop registration down")
}
