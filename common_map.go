package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// doMap implements a map management function that reads content from an input file
// splits the output into a specified number of intermediate files, and processes the content
// according to a custom splitting standard.
func doMap(
	jobName jobParse,
	mapTaskNumber int,
	inputFile string,
	nReduce int,
	mapF func(string, string) []KeyValue,
) {
	// Read content from the input file inputFile
	content, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("Read the content of file failed %v\n", err)
	}

	// Process the content by calling mapF and split the output into map task results
	kvs := mapF(inputFile, string(content))

	// Create nReduce number of encoders for intermediate files
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// Generate the name for the intermediate file
		fileName := reduceName(jobName, mapTaskNumber, i)
		f, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("create file [%s] failed with error : %v\n", fileName, err)
		}
		defer f.Close()
		encoders[i] = json.NewEncoder(f)
	}

	// Classify the key-value pairs into the nReduce files based on hash values
	for _, v := range kvs {
		index := ihash(v.Key) % nReduce
		if err := encoders[index].Encode(&v); err != nil {
			log.Fatalf("Unable to write to file\n")
		}
	}
}
