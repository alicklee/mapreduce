package main

import (
	"fmt"
	"log"
	"mapreduce"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// jobParse 是 mapreduce 包中的类型
type jobParse = mapreduce.JobParse

func main() {
	//获取项目根目录
	rootDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// 修复路径拼接，移除多余的 ./
	inputDir := filepath.Join(rootDir, strings.TrimPrefix(mapreduce.Config["input"], "./"))

	// 确保输入目录存在
	if err := os.MkdirAll(inputDir, 0777); err != nil {
		log.Fatal(err)
	}

	// 创建示例输入文件，使用 filepath.Join 来正确拼接路径
	inputFile1 := filepath.Join(inputDir, "example1.txt")
	inputFile2 := filepath.Join(inputDir, "example2.txt")

	fmt.Println("Input files created:")
	fmt.Println(inputFile1)
	fmt.Println(inputFile2)

	// 初始化并启动 Master 节点
	fmt.Println("Starting master node...")

	// 配置 MapReduce 任务
	inputFiles := []string{inputFile1, inputFile2}
	nReduce := len(inputFiles)                        // reduce 任务数量
	masterSocket := mapreduce.Config["master_socket"] // 使用配置文件中的 socket 地址

	// 确保 socket 目录存在
	socketDir := mapreduce.Config["socket_base"]
	if err := os.MkdirAll(socketDir, 0777); err != nil {
		log.Fatalf("Failed to create socket directory: %v", err)
	}

	// 清理可能存在的旧 socket 文件
	if err := os.Remove(masterSocket); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove old master socket: %v", err)
	}

	fmt.Printf("Master socket: %s\n", masterSocket)
	fmt.Printf("Number of reduce tasks: %d\n", nReduce)
	fmt.Printf("Input files: %v\n", inputFiles)

	// 创建并启动 master
	fmt.Println("Creating and starting master...")
	master := mapreduce.Distributed(jobParse("wordcount"), inputFiles, nReduce, masterSocket)
	if master == nil {
		log.Fatal("Failed to create master")
	}

	fmt.Println("Waiting for workers to connect...")

	// 创建一个通道用于接收中断信号
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// 在后台等待任务完成
	done := make(chan struct{})
	go func() {
		master.Wait()
		close(done)
	}()

	// 等待任务完成或中断信号
	select {
	case <-done:
		fmt.Println("All tasks completed successfully")
	case <-interrupt:
		fmt.Println("\nReceived interrupt signal. Shutting down...")
		// 给 master 一些时间来清理资源
		time.Sleep(time.Second)
	}

	fmt.Println("Master node completed")
	fmt.Println("Results can be found in: ./assets/result/mrt.result.txt")
}
