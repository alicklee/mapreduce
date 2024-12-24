package mapreduce

import (
	"hash/fnv"
	"strconv"
)

type jobParse string

const (
	mapParse    jobParse = "Map"
	reduceParse jobParse = "Reduce"
)

// Constants for RPC method names
const (
	RegisterMethod = "Master.Register" // Method name for worker registration
	DoTaskMethod   = "Worker.DoTask"   // Method name for task execution
	ShutdownMethod = "Worker.Shutdown" // Method name for worker shutdown
)

// 用于保存需要传递给map和reduce的KV数据对
type KeyValue struct {
	Key   string
	Value string
}

func mergeName(jobName jobParse, reduceTask int) string {
	return "./assets/result/mrtmp." + string(jobName) + "-res-" + strconv.Itoa(reduceTask)
}

func reduceName(jobName jobParse, mapTaskNumber int, reduceTask int) string {
	return "./assets/output/mrtmp." + string(
		jobName,
	) + "-" + strconv.Itoa(
		mapTaskNumber,
	) + "-" + strconv.Itoa(
		reduceTask,
	)
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7ffffff)
}
