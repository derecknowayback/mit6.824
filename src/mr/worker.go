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
)

func init() {
	log.SetPrefix("Worker-" + strconv.Itoa(os.Getpid()%3) + "   ")
}

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

	// Your worker implementation here.
	for {
		if nReduce == -1 {
			nReduce = AskForR()
			if nReduce == -1 {
				continueFailure++
			}
		}
		log.Println(fmt.Sprintf("nReduce %d", nReduce))
		// 请求任务
		job := AskForJob()
		if job == nil {
			continueFailure++
		} else if job.Type == NoJobType {
			// 如果没有任务 sleep 1s (这里master可以给更多的信息)
			time.Sleep(time.Second)
		} else if job.Type == MapJobType {
			// map任务: 根据key(这边是文件名,来hash分区) 输出中间文件到本地,中间文件的格式用json/csv来存储
			inputStr := string(job.Data)
			kvs := mapf(job.Name, inputStr)
			for _, pair := range kvs {
				hash := ihash(pair.Key) % nReduce
				csvWrapper, ok := middleFileMap[hash]
				if !ok {
					fd, err := os.CreateTemp("", "temp-mid-*")
					if err != nil {
						log.Println(fmt.Sprintf("Create file %s error: %v", fd.Name(), err))
						continue
					}
					// 现在把kv写入
					csvWrapper = &CSVWrapper{writer: csv.NewWriter(fd), file: fd}
					middleFileMap[hash] = csvWrapper
				}
				err := csvWrapper.writer.Write([]string{pair.Key, pair.Value})
				if err != nil {
					log.Println(fmt.Sprintf("CSV Write error: %v", err))
				}
			}

			SignalJobFinished(*job)
			// 收尾
			for hash, wrapper := range middleFileMap {
				middleFileName := fmt.Sprintf(MiddleFileFormat, hash, job.Name)
				err := os.Rename(wrapper.file.Name(), middleFileName)
				if err != nil {
					log.Printf("Rename file error: %v\n", err)
				}
				wrapper.file.Close()
			}
			middleFileMap = map[int]*CSVWrapper{}
		} else if job.Type == ReduceJobType {
			// reduce: 读取中间文件, 存到内存里,
			jobIndex, err := strconv.Atoi(job.Name)
			if err != nil || jobIndex > nReduce || jobIndex < 0 {
				log.Println(fmt.Sprintf("Invalid job index:%d or err: %v", jobIndex, err))
			}

			dir, err := os.ReadDir(".")
			if err != nil {
				log.Printf("read dir error:%v\n", err)
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
					log.Println(fmt.Sprintf("Open file %s error: %v", name, err))
					continue
				}
				csvReader := csv.NewReader(fd)
				// 将csv转为kv
				var kvs ByKey
				for {
					line, err := csvReader.Read()
					if err != nil {
						if errors.Is(io.EOF, err) {
							break
						} else {
							log.Println(fmt.Sprintf("Read csv error: %v", err))
						}
					}
					kvs = append(kvs, KeyValue{Key: line[0], Value: line[1]})
				}

				sort.Sort(kvs)

				outputFileName := OutputFilePrefix + strconv.Itoa(jobIndex)
				fd, err = os.Create(outputFileName)
				if err != nil {
					log.Println(fmt.Sprintf("Create file %s error: %v", outputFileName, err))
					continue
				}

				i := 0
				for i < len(kvs) {
					j := i + 1
					var values []string
					values = append(values, kvs[i].Value)
					for j < len(kvs) && kvs[j].Key == kvs[i].Key {
						j++
						values = append(values, kvs[j].Value)
					}
					output := reducef(kvs[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(fd, "%v %v\n", kvs[i].Key, output)
					i = j
				}
			}
			SignalJobFinished(*job)
		} else {
			log.Println(fmt.Sprintf("Warning Unknown Type: %s", job.Type))
			continueFailure++
		}

		if continueFailure == FailThreadHold {
			os.Exit(0)
		}
	}

}

// AskForJob asks the master for a job, map or reduce
func AskForJob() *Job {
	job := Job{}
	ok := call("Coordinator.AssignJob", nil, &job)
	if !ok {
		log.Println("can't get a job...")
		return nil
	}
	return &job
}

func AskForR() int {
	var r int
	ok := call("Coordinator.GetR", nil, &r)
	if !ok {
		log.Println("can't get a r...")
		return -1
	}
	return r
}

func SignalJobFinished(job Job) {
	ok := call("Coordinator.GetJobDone", &job, nil)
	if !ok {
		log.Println("can't finish a job...")
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
