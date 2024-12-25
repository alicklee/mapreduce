// Package mapreduce implements a distributed MapReduce framework
package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
)

// ResultMerger handles the final merge phase of MapReduce results
type ResultMerger struct {
	jobName    JobParse
	nReduce    int
	resultDir  string
	resultFile string
	results    map[string][]string
}

// NewResultMerger creates a new instance for merging results
func NewResultMerger(jobName JobParse, nReduce int) *ResultMerger {
	return &ResultMerger{
		jobName:    jobName,
		nReduce:    nReduce,
		resultDir:  Config["result"],
		resultFile: filepath.Join(Config["result"], "mrt.result.txt"),
		results:    make(map[string][]string),
	}
}

// Merge combines all reduce task outputs into a single result file
func (mr *Master) merge() {
	merger := NewResultMerger(mr.jobName, mr.nReduce)
	if err := merger.Execute(); err != nil {
		log.Printf("Merge failed: %v", err)
	}
}

// Execute performs the merge operation
func (m *ResultMerger) Execute() error {
	if err := m.prepareResultDirectory(); err != nil {
		return fmt.Errorf("failed to prepare result directory: %v", err)
	}

	if err := m.collectReduceOutputs(); err != nil {
		return fmt.Errorf("failed to collect reduce outputs: %v", err)
	}

	if err := m.writeResults(); err != nil {
		return fmt.Errorf("failed to write final results: %v", err)
	}

	return nil
}

// prepareResultDirectory ensures the result directory exists
func (m *ResultMerger) prepareResultDirectory() error {
	// Debug log to print the result directory path
	log.Printf("Configured result directory path: %s", m.resultDir)

	return os.MkdirAll(m.resultDir, 0777)
}

// collectReduceOutputs reads and combines all reduce task outputs
func (m *ResultMerger) collectReduceOutputs() error {
	for i := 0; i < m.nReduce; i++ {
		fileName := mergeName(m.jobName, i)
		fmt.Printf("Merge: reading %s\n", fileName)

		if err := m.processReduceOutput(fileName); err != nil {
			log.Printf("Warning: error processing %s: %v", fileName, err)
			continue
		}
	}
	return nil
}

// processReduceOutput reads and processes a single reduce output file
func (m *ResultMerger) processReduceOutput(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break // End of file or error
		}
		m.results[kv.Key] = append(m.results[kv.Key], kv.Value)
	}
	return nil
}

// writeResults writes the merged results to the final output file
func (m *ResultMerger) writeResults() error {
	file, err := os.Create(m.resultFile)
	if err != nil {
		return fmt.Errorf("failed to create result file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Get sorted keys for deterministic output
	keys := m.getSortedKeys()

	// Write each key and its values
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "%s: %v\n", key, m.results[key]); err != nil {
			return fmt.Errorf("failed to write result: %v", err)
		}
	}

	return nil
}

// getSortedKeys returns a sorted slice of all keys
func (m *ResultMerger) getSortedKeys() []string {
	keys := make([]string, 0, len(m.results))
	for key := range m.results {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
