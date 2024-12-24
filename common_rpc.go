package mapreduce

import "net/rpc"

type ShutdownReply struct {
	Ntasks int // The job number which workers finished
}

/*
*
 1. srv: address
 2. rpcName
 3. args

*
*/
func call(srv string, rpcName string, args interface{}, reply interface{}) bool {
	// connect rpc service
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()
	if err := c.Call(rpcName, args, reply); err != nil {
		return false
	}
	return true
}

type RegisterArgs struct {
	Worker string // worker's address
}
