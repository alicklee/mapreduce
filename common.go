// Package mapreduce implements a distributed MapReduce framework
// that supports both sequential and distributed execution modes.
package mapreduce

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
)

// KeyValue represents a key-value pair emitted by Map functions
// and processed by Reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// JobParse represents the type of job phase in the MapReduce framework
type JobParse string

const (
	// mapParse represents the Map phase of MapReduce
	mapParse JobParse = "Map"

	// reduceParse represents the Reduce phase of MapReduce
	reduceParse JobParse = "Reduce"
)

// mergeName constructs the name of an intermediate file that
// stores the intermediate data for a specific reduce task.
//
// Parameters:
//   - jobName: The name of the MapReduce job
//   - id: The ID of the reduce task
//
// Returns the constructed file name.
func mergeName(jobName JobParse, reduceTask int) string {
	// Use paths from the Config variable
	outDir := Config["output"]

	// Ensure the output directory exists
	if err := os.MkdirAll(outDir, 0777); err != nil {
		log.Printf("Failed to create output directory: %v", err)
	}

	return fmt.Sprintf("%s/mrtmp.%v-%d", outDir, jobName, reduceTask)
}

func reduceName(jobName JobParse, mapTaskNumber int, reduceTask int) string {
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
