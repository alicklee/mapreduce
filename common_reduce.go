package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// manage reduce job
func doReduce(
	jobName jobParse,
	reduceTaskNumber int,
	outFile string,
	nMap int,
	reduceF func(string, []string) string,
) {
	var result map[string][]string = make(map[string][]string)

	// open every tmp file
	for i := 0; i < nMap; i++ {
		inputFile := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("open file [%s] with error : $v\n", inputFile)
		}
		defer f.Close()

		// get content
		decoder := json.NewDecoder(f)
		var kv KeyValue
		for decoder.More() {
			err := decoder.Decode(&kv)
			if err != nil {
				log.Fatalf("decode with error %v\n", err)
			}
			// merge  contents with same key
			result[kv.Key] = append(result[kv.Key], kv.Value)
		}
	}

	// handle the content

	var keys []string
	for key := range result {
		keys = append(keys, key)
	}

	// create the result file
	f, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("create file with error : %v\n", err)
	}

	defer f.Close()
	encoder := json.NewEncoder(f)
	for _, key := range keys {
		err := encoder.Encode(KeyValue{key, reduceF(key, result[key])})
		if err != nil {
			log.Fatalf("encode with error %v\n", err)
		}

	}
}
