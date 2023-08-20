package mr

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type CSVWrapper struct {
	writer *csv.Writer
	file   *os.File
}

const (
	MiddleFileFormat = "middle-file-%d-%s"
	OutputFilePrefix = "mr-out-"
	FailThreadHold   = 20
	NoJobThreadHold  = 20
)

var logger = log.New(os.Stdout, "Worker-"+strconv.Itoa(os.Getpid()%3)+"   ", log.LstdFlags)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// The workers will talk to the master via RPC.
//Each worker process will ask the master for a task,
//read the task's input from one or more files,
//execute the task,
//and write the task's output to one or more files.
//When the job is completely finished, the worker processes should exit.

// main/mrworker.go calls this function.
// it shoud be a loop function. Exit when the master is over or notified by a over signal.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	nReduce := -1

	middleFileMap := map[int]*CSVWrapper{}

	continueFailure := 0
	noJob := 0

	// Your worker implementation here.
	for {
		if nReduce == -1 {
			nReduce = AskForR()
			if nReduce == -1 {
				continueFailure++
			}
		}
		//log.Println(fmt.Sprintf("nReduce %d", nReduce))
		// 请求任务
		job := AskForJob()
		if job == nil {
			continueFailure++
		} else if job.Type == NoJobType {
			// 如果没有任务 sleep 1s (这里master可以给更多的信息)
			time.Sleep(time.Second)
			noJob++
		} else if job.Type == MapJobType {
			noJob = 0
			continueFailure = 0
			// map任务: 根据key(这边是文件名,来hash分区) 输出中间文件到本地,中间文件的格式用json/csv来存储
			inputStr := string(job.Data)
			kvs := mapf(job.Index, inputStr)
			for _, pair := range kvs {
				hash := ihash(pair.Key) % nReduce
				csvWrapper, ok := middleFileMap[hash]
				if !ok {
					fd, err := os.CreateTemp("", "temp-mid-*")
					if err != nil {
						logger.Println(fmt.Sprintf("Create file %s error: %v", fd.Name(), err))
						continue
					}
					// 现在把kv写入
					csvWrapper = &CSVWrapper{writer: csv.NewWriter(fd), file: fd}
					middleFileMap[hash] = csvWrapper
				}
				err := csvWrapper.writer.Write([]string{pair.Key, pair.Value})
				if err != nil {
					logger.Println(fmt.Sprintf("CSV Write error: %v", err))
				}
			}
			// 收尾
			for hash, wrapper := range middleFileMap {
				wrapper.writer.Flush()
				middleFileName := fmt.Sprintf(MiddleFileFormat, hash, job.Name)
				err := os.Rename(wrapper.file.Name(), middleFileName)
				if err != nil {
					logger.Printf("Rename file error: %v\n", err)
				}
				wrapper.file.Close()
			}
			SignalJobFinished(*job)
			middleFileMap = map[int]*CSVWrapper{}
		} else if job.Type == ReduceJobType {
			noJob = 0
			continueFailure = 0
			// reduce: 读取中间文件, 存到内存里,
			jobIndex, err := strconv.Atoi(job.Name)
			if err != nil || jobIndex > nReduce || jobIndex < 0 {
				logger.Println(fmt.Sprintf("Invalid job index:%d or err: %v", jobIndex, err))
			}

			var kvs ByKey
			dir, err := os.ReadDir(".")
			if err != nil {
				logger.Printf("read dir error:%v\n", err)
				continue
			}
			prefix := "middle-file-" + job.Name
			for _, d := range dir {
				if d.IsDir() {
					continue
				}
				name := d.Name()
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				fd, err := os.Open(name)
				if err != nil {
					logger.Println(fmt.Sprintf("Open file %s error: %v", name, err))
					continue
				}
				csvReader := csv.NewReader(fd)
				// 将csv转为kv
				for {
					line, err := csvReader.Read()
					if err != nil {
						if errors.Is(io.EOF, err) {
							break
						} else {
							logger.Println(fmt.Sprintf("Read csv file %s error: %v", fd.Name(), err))
						}
					}
					kvs = append(kvs, KeyValue{Key: line[0], Value: line[1]})
				}
				fd.Close()
			}
			sort.Sort(kvs)
			logger.Println(fmt.Sprintf("job-%s kvs -- %v", job.Name, kvs))
			outputFileName := OutputFilePrefix + strconv.Itoa(jobIndex)
			fd, err := os.Create(outputFileName)
			if err != nil {
				logger.Println(fmt.Sprintf("Create file %s error: %v", outputFileName, err))
				continue
			}
			i := 0
			for i < len(kvs) {
				j := i + 1
				var values []string
				values = append(values, kvs[i].Value)
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					values = append(values, kvs[j].Value)
					j++
				}
				output := reducef(kvs[i].Key, values)
				// this is the correct format for each line of Reduce output.
				_, err := fmt.Fprintf(fd, "%v %v\n", kvs[i].Key, output)
				logger.Println(fmt.Sprintf("write %v %v", kvs[i].Key, output))
				if err != nil {
					logger.Println(fmt.Sprintf("write output-file err: %v", err))
				}
				i = j
			}
			SignalJobFinished(*job)
			for _, d := range dir {
				if d.IsDir() {
					continue
				}
				name := d.Name()
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				err = os.Remove(name)
				if err != nil {
					logger.Println(fmt.Sprintf("Remove middle file err: %v", err))
				}
			}
		} else {
			logger.Println(fmt.Sprintf("Warning Unknown Type: %s", job.Type))
			continueFailure++
		}
		if continueFailure == FailThreadHold {
			logger.Println("Too much failure, worker exit ...")
			os.Exit(0)
		}
		if noJob == NoJobThreadHold {
			logger.Println("No job exist, worker exit...")
			os.Exit(0)
		}
	}

}

// AskForJob asks the master for a job, map or reduce
func AskForJob() *Job {
	job := Job{}
	nothing := Nothing(0)
	ok := call("Coordinator.AssignJob", &nothing, &job)
	if !ok {
		logger.Println("can't get a job...")
		return nil
	}
	return &job
}

func AskForR() int {
	var r int
	nothing := Nothing(0)
	ok := call("Coordinator.GetR", &nothing, &r)
	if !ok {
		logger.Println("can't get a r...")
		return -1
	}
	return r
}

func SignalJobFinished(job Job) {
	logger.Println(fmt.Sprintf("job-%s done: %s", job.Name, job.ToStr()))
	ok := call("Coordinator.GetJobDone", &job, nil)
	if !ok {
		logger.Println("can't finish a job...")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logger.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
