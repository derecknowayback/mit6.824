package mr

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
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

//var masterLogger = log.New(os.Stdout, "Master   ", log.LstdFlags)

type Coordinator struct {
	// Your definitions here.
	files         []string
	nReduce       int
	JobList       map[string]Job
	lock          sync.Mutex
	ProduceReduce bool
}

// Your code here -- RPC handlers for the worker to call.
// AssignJob handles worker's request for a job
func (c *Coordinator) AssignJob(nothing Nothing, job *Job) error {
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

	for key, job := range c.JobList {
		if !isJobTimeOut(job) && !job.Assigned {
			job.Assigned = true
			job.Start = now
			c.JobList[key] = job
			res = c.JobList[key]
			found = true
			break
		}
	}

	// 如果所有map-job都结束了,产生reduce-job
	if !found && len(c.JobList) == 0 && !c.ProduceReduce {
		c.JobList = map[string]Job{}
		for i := 0; i < c.nReduce; i++ {
			c.JobList[strconv.Itoa(i)] = Job{Type: ReduceJobType, Name: strconv.Itoa(i), Assigned: false}
		}
		c.ProduceReduce = true
		found = true
		res = c.JobList["0"]
		res.Assigned = true
		res.Start = now
		c.JobList["0"] = res
	}

	//log.Println("The JobList are: ")
	//log.Println(c.GetJobStr())

	// unlock
	c.lock.Unlock()
	if !found {
		//log.Println("No find job ...")
		*job = Job{Type: NoJobType}
		return nil
	}

	*job = res
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

func (c *Coordinator) GetJobDone(job Job, nothing *Nothing) error {
	// todo bug -> 有一个问题是, 假设我现在worker1太慢了超时了,我把工作assign给了另外一个worker2,ok,但是这时候突然又收到worker1的结束信号,豁
	// 这不是会重复一个工作吗?
	c.lock.Lock()
	log.Println(fmt.Sprintf("Job-%s done: %s", job.Name, job.ToStr()))
	delete(c.JobList, job.Name+job.Index)
	c.lock.Unlock()
	return nil
}

// GetR tells the worker nReduce
func (c *Coordinator) GetR(nothing Nothing, k *int) error {
	*k = c.nReduce
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
	isJobTimeOut := func(job Job) bool {
		now := time.Now()
		return job.Start.Add(DefaultTimeout).Before(now)
	}
	for {
		log.Println("Check TIMEOUT")
		// todo: 给锁上优先级 https://stackoverflow.com/questions/11666610/how-to-give-priority-to-privileged-thread-in-mutex-locking
		c.lock.Lock()
		for key, job := range c.JobList {
			if !job.Start.IsZero() && isJobTimeOut(job) {
				log.Println(fmt.Sprintf("Job-%s time out: %s", job.Name, job.ToStr()))
				job.Assigned = false
				job.Start = time.Time{}
				c.JobList[key] = job
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second * 3)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//return false
	c.lock.Lock()
	defer c.lock.Unlock()
	res := c.ProduceReduce && len(c.JobList) == 0
	//log.Println(fmt.Sprintf("cluster joblist: %s", c.GetJobStr()))
	return res
}

func (c *Coordinator) GetJobStr() string {
	var res []string
	for _, job := range c.JobList {
		res = append(res, job.ToStr())
	}
	return strings.Join(res, "\n")
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
			Index:    fileName,
			Type:     MapJobType,
			Data:     content,
			Assigned: false,
			Start:    time.Time{},
		}
		c.JobList[key+fileName] = job
	}

	c.server()
	go c.CheckJobTimeOut()
	return &c
}
