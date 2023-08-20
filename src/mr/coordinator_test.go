package mr

import (
	"os"
	"strings"
	"testing"
)

func TestMakeCoordinator(t *testing.T) {
	coordinator, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 10)
	if err != nil {
		t.Fatal(err)
	}

	if coordinator.nReduce != 10 {
		t.Fatal("nReduce wrong")
	}

	if len(coordinator.JobList) != 8 {
		t.Fatal("JobList count dismatch")
	}
}

func TestCoordinator_AssignJob_MapJob(t *testing.T) {
	coordinator, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 10)
	if err != nil {
		t.Fatal(err)
	}
	job := Job{}
	err = coordinator.AssignJob(0, &job)
	if err != nil {
		t.Fatal(err)
	}

	if !job.Assigned {
		t.Fatal("Job assign wrong ...")
	}

	if job.Type != MapJobType {
		t.Fatal("Wrong job type ...")
	}

	if job.Start.IsZero() {
		t.Fatal("Forget to set time ...")
	}

}

func TestCoordinator_AssignJob_ReduceJob(t *testing.T) {
	coordinator, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 10)
	if err != nil {
		t.Fatal(err)
	}

	// clear JobList
	coordinator.JobList = map[string]Job{}

	job := Job{}
	err = coordinator.AssignJob(0, &job)
	if err != nil {
		t.Fatal(err)
	}

	if job.Type != ReduceJobType {
		t.Fatalf("Wrong job type: %s", job.Type)
	}

	if job.Start.IsZero() {
		t.Fatal("Forget to set time ...")
	}

	if !job.Assigned {
		t.Fatal("Job assign wrong ...")
	}

	firstName := job.Name

	err = coordinator.AssignJob(0, &job)
	if err != nil {
		t.Fatal(err)
	}

	if firstName == job.Name {
		t.Fatal("Re-assign same job ...")
	}

}

func MockCoordinator(dirName string, nReduce int) (*Coordinator, error) {
	inputDir, err := os.ReadDir(dirName)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range inputDir {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".txt") {
			files = append(files, strings.Join([]string{dirName, entry.Name()}, "/"))
		}
	}

	coordinator := MakeCoordinator(files, nReduce)
	return coordinator, nil
}
