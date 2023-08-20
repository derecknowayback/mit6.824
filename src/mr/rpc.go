package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

const (
	DefaultTimeout         = time.Second * 10
	MapJobType     JobType = "Map"
	ReduceJobType  JobType = "Reduce"
	NoJobType      JobType = "NoJob"
)

// Add your RPC definitions here.
type JobType string
type Nothing int

type Job struct {
	Name     string
	Index    string
	Start    time.Time
	Type     JobType
	Data     []byte
	Assigned bool
}

func (j *Job) ToStr() string {
	return fmt.Sprintf("{ Name:[%s]  Index:[%s]  Type:[%s]  Assigned:[%v] Start:[%v]}", j.Name, j.Index, j.Type, j.Assigned, j.Start)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
