package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
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

type Job struct {
	Name     string
	Start    time.Time
	Type     JobType
	Data     []byte
	Assigned bool
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
