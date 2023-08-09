package mr

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// The master should notice if a worker hasn't completed its task in a reasonable amount of time
//(for this lab, use ten seconds), and give the same task to a different worker.

//todo: 1. 我们现在没法解决假宕机: worker-A没有宕机,只是超时了,Job调度给worker-B,那么最后就会有两个人同时执行这件事
//      2. worker缺乏一个elegant的问询master是否完蛋的机制

type Coordinator struct {
	// Your definitions here.
	files         []string
	nReduce       int
	JobList       map[string]Job
	lock          sync.Mutex
	ProduceReduce bool
}

func init() {
	log.SetPrefix("Master   ")
}

// Your code here -- RPC handlers for the worker to call.
// AssignJob handles worker's request for a job
func (c *Coordinator) AssignJob(nothing interface{}, job *Job) error {
	log.Println("Assigning job now ...")
	now := time.Now()
	isJobTimeOut := func(job Job) bool {
		if job.Start.IsZero() {
			return false
		}
		return job.Start.Add(DefaultTimeout).Before(now)
	}
	var res Job
	var found bool
	// lock
	c.lock.Lock()

	for name, job := range c.JobList {
		if !isJobTimeOut(job) && !job.Assigned {
			job.Assigned = true
			job.Start = now
			c.JobList[name] = job
			res = c.JobList[name]
			found = true
			break
		}
	}

	// 如果所有map-job都结束了,产生reduce-job
	if !found && len(c.JobList) == 0 && !c.ProduceReduce {
		c.ProduceReduce = true
		c.JobList = map[string]Job{}
		for i := 0; i < c.nReduce; i++ {
			c.JobList[strconv.Itoa(i)] = Job{Type: ReduceJobType, Name: strconv.Itoa(i), Assigned: false}
		}

		found = true
		res = c.JobList["0"]
		res.Assigned = true
		res.Start = now
		c.JobList["0"] = res
	}

	// unlock
	c.lock.Unlock()
	if !found {
		log.Println("No find job ...")
		*job = Job{Type: NoJobType}
		return nil
	}

	*job = res
	log.Println(fmt.Sprintf("job assigned:%v", res))
	return nil
}

// IsMapJobAllOver tells the worker is there map job running
func (c *Coordinator) IsMapJobAllOver() bool {
	for _, v := range c.JobList {
		if v.Type == MapJobType {
			return false
		}
	}
	return true
}

func (c *Coordinator) GetJobDone(job Job, nothing interface{}) error {
	// todo bug -> 有一个问题是, 假设我现在worker1太慢了超时了,我把工作assign给了另外一个worker2,ok,但是这时候突然又收到worker1的结束信号,豁
	// 这不是会重复一个工作吗?
	delete(c.JobList, job.Name)
	return nil
}

// GetR tells the worker nReduce
func (c *Coordinator) GetR(nothing interface{}, k *int) error {
	*k = c.nReduce
	log.Println("in in in in in in in in ~~~~~~~~~~~~~")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) CheckJobTimeOut() {
	now := time.Now()
	isJobTimeOut := func(job Job) bool {
		return job.Start.Add(DefaultTimeout).Before(now)
	}
	for {
		for name, job := range c.JobList {
			if !job.Start.IsZero() && isJobTimeOut(job) {
				job.Assigned = false
				job.Start = time.Time{}
				c.JobList[name] = job
			}
		}
		time.Sleep(time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.ProduceReduce && len(c.JobList) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(_files []string, _nReduce int) *Coordinator {
	c := Coordinator{files: _files, nReduce: _nReduce, JobList: map[string]Job{}}

	// Your code here.
	// read the file
	for _, fileName := range c.files {
		fd, err := os.Open(fileName)
		if err != nil {
			log.Println(fmt.Sprintf("can't open file %s err:%v", fileName, err))
			return nil
		}
		content, err := io.ReadAll(fd)
		if err != nil {
			log.Println(fmt.Sprintf("read file err:%v", err))
			return nil
		}
		fd.Close()

		// give input to map
		key := strconv.Itoa(len(c.JobList))
		job := Job{
			Name:     key,
			Type:     MapJobType,
			Data:     content,
			Assigned: false,
			Start:    time.Time{},
		}
		c.JobList[key] = job
	}

	c.server()
	go c.CheckJobTimeOut()
	return &c
}
