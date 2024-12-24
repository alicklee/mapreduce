// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

// doMap manages the map phase of a MapReduce job.
// It reads input data, applies the map function, and partitions the results
// into intermediate files for the reduce phase.
//
// The map phase works as follows:
// 1. Reads the entire input file into memory
// 2. Applies the user's map function to generate key-value pairs
// 3. Partitions the pairs across nReduce intermediate files
// 4. Writes each partition using JSON encoding
//
// Parameters:
//   - jobName: Unique identifier for the MapReduce job
//   - mapTaskNumber: Index of this map task (0-based)
//   - inFile: Path to the input file to process
//   - nReduce: Number of reduce tasks (determines number of partitions)
//   - mapF: User-defined function to generate key-value pairs
//
// Error handling:
//   - Fatally exits if the input file cannot be read
//   - Fatally exits if intermediate files cannot be created
//   - Fatally exits if JSON encoding fails
//
// The intermediate files use JSON encoding to ensure reliable
// data transfer between map and reduce phases.
func doMap(
	jobName jobParse,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(string, string) []KeyValue,
) {
	// Read the entire input file into memory
	// This simplifies the map function interface
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("doMap: open file %s error %v", inFile, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("doMap: read file %s error %v", inFile, err)
	}

	// Apply the user's map function to generate key-value pairs
	// The function processes the entire file content at once
	kva := mapF(inFile, string(content))

	// Create encoders and files for each reduce partition
	// Each encoder will handle key-value pairs for one reducer
	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		file, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		if err != nil {
			log.Fatalf("doMap: create file error %v", err)
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
		files[i] = file
	}

	// Partition map output by hashing each key
	// This distributes the work evenly across reducers
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("doMap: encode error %v", err)
		}
	}
}
