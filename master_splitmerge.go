package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

func (mr *Master) merge() {
	result := make(map[string][]string)
	for i := 0; i < mr.nReduce; i++ {
		fileName := mergeName(mr.jobName, i)
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Read file [%s] with error : %v\n", fileName, err)
		}
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
		f.Close()
	}

	var keys []string
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// create the result file
	f, err := os.Create("./assets/result/mrt.result.txt")
	if err != nil {
		log.Fatalf("create file with error : %v\n", err)
	}

	defer f.Close()
	w := bufio.NewWriter(f)
	for _, k := range keys {
		fmt.Fprintf(w, "%s\n", k)
	}
	w.Flush()
}
