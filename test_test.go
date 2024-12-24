package mapreduce

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	nNumber = 100
)

// Create a file include N numbers
// Use MapReduce handle the file
// Check the output file has N numbers

func MapFunc(file string, value string) (res []KeyValue) {
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

func ReduceFunc(key string, values []string) string {
	for _, element := range values {
		fmt.Printf("Reduce %s-%v\n", key, element)
	}
	return ""
}

// func TestSequentialSignle(t *testing.T) {
// 	Sequential(mapParse, makeInputs(1), 1, MapFunc, ReduceFunc)
// }

func TestSequentialMany(t *testing.T) {
	Sequential(mapParse, makeInputs(5), 3, MapFunc, ReduceFunc)
}

// 创建输入文件
// 根据指定的数量创建文件，返回创建好的文件名列表
func makeInputs(num int) []string {
	var names []string
	i := 0
	for f := 0; f < num; f++ {
		// Create file name by MIT6.824
		names = append(names, fmt.Sprintf("./assets/input/824-mrinput-%d.txt", f))
		// Create file
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatalf("create input file %s failed. error: %v", names[f], err)
		}
		w := bufio.NewWriter(file)
		// mock num个文件，并写入不同的数据
		for i < (f+1)*(nNumber/num) {
			// wirte i to buffer
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		// write buffer to file
		w.Flush()
		file.Close()
	}
	return names
}
